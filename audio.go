/* audio —— 那 20 字节的头，和「一秒一百包」这件事
   play-agent · 广播控制台

   网页把麦克风 PCM 送过来（16k / 16 位 / 小端 / 单声道），这里给它套上
   20 字节的自定义头，用 UDP 发到打铃服务器的 51340。**头里没有目标信息** ——
   发给谁是由控制通道那条 collect_pcm 里的 EndPointsList 决定的，服务器按
   HashKey 认领这路流再自己扇出。所以这一层只管发，不管发给谁。

   20 字节头（对着抓包逐字节抄的）：

     偏移  长度  内容
     0     4     本机 IPv4 四段原值           ← 必须和 UDP 的真实源地址一致
     4     2     本机源端口（大端）           ← 同上
     6     2     固定 04 22
     8     4     序号，从 0 起（大端）
     12    4     固定 7f ff ff ff
     16    2     本帧 PCM 字节数 = 320（大端）
     18    2     自增计数（大端）

   前 6 个字节和 HashKey 说的是同一件事：**这条 UDP 套接字是谁**。所以顺序是
   死的 —— 先把套接字建出来、connect、问出真实 ip:port，才能拼 HashKey 去发
   collect_pcm；写死一个端口是不行的（每次绑定拿到的都不一样，抓两次包就是
   两个数：52470、57966）。
*/

package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const (
	pcmBytes    = 320                   /* 每包的 PCM 字节数，= 10ms 的 16k/16 位单声道 */
	pktEvery    = 10 * time.Millisecond /* 100 包/秒 */
	audioBufMax = 320 * 100             /* 缓冲上限：一秒。超了就丢最旧的 */
)

type audioSender struct {
	conn    *net.UDPConn
	hashKey string /* "ip:port"，要塞进 collect_pcm 的 HashKey */

	srcIP   [4]byte
	srcPort uint16

	mu      sync.Mutex
	buf     []byte /* 网页送来还没发出去的 PCM */
	muted   bool
	seq     uint32
	counter uint16

	quit chan struct{}
	once sync.Once
}

/*
newAudioSender 建一条 UDP 套接字指向 target（形如 192.168.1.100:51340），

	问出它真实的本地地址，然后开始按 100 包/秒往外泵。

	**用 Dial 而不是 Listen + WriteTo**：只有 connect 过的套接字才问得到
	内核给的那个本地 ip:port（未 connect 的 UDP 套接字 address() 是 0.0.0.0:0），
	而那个 ip:port 正是 HashKey。顺带还省了每次 write 都要填目标地址。
*/
func newAudioSender(target string) (*audioSender, error) {
	raddr, err := net.ResolveUDPAddr("udp4", target)
	if err != nil {
		return nil, fmt.Errorf("对讲地址无效（%s）：%v", target, err)
	}
	conn, err := net.DialUDP("udp4", nil, raddr)
	if err != nil {
		return nil, fmt.Errorf("创建 UDP 套接字失败：%v", err)
	}
	la, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("无法获取本地地址")
	}
	ip4 := la.IP.To4()
	if ip4 == nil {
		_ = conn.Close()
		return nil, fmt.Errorf("本地地址不是 IPv4（%s）：音频头只能承载 4 字节地址", la)
	}

	a := &audioSender{
		conn:    conn,
		srcPort: uint16(la.Port),
		hashKey: fmt.Sprintf("%s:%d", ip4.String(), la.Port),
		quit:    make(chan struct{}),
	}
	copy(a.srcIP[:], ip4)

	go a.pump()
	return a, nil
}

/*
push 收下网页送来的一段 PCM。不走 muted 分支 —— 静音是**发送端**的事：

	停在这里不动，泵那头照样按 100 包/秒填静音，UDP 流一直是热的，
	服务器不会因为「突然没包了」把这路任务判掉。
*/
func (a *audioSender) push(p []byte) {
	if len(p) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.muted {
		return
	}
	a.buf = append(a.buf, p...)
	/* 网页那头比我们快的时候（标签页被节流后一口气补发之类），
	   只留最近的一秒 —— 老音频晚到不如不到。 */
	if n := len(a.buf) - audioBufMax; n > 0 {
		a.buf = append(a.buf[:0], a.buf[n:]...)
	}
}

func (a *audioSender) setMuted(on bool) {
	a.mu.Lock()
	a.muted = on
	if on {
		a.buf = nil
	}
	a.mu.Unlock()
}

func (a *audioSender) stop() {
	a.once.Do(func() { close(a.quit) })
	_ = a.conn.Close()
}

/*
pump 是那口钟。

	**不用「每 10ms 醒一次就发一包」那种写法**：Windows 上定时器的分辨率
	不一定给得到 10ms（Go 会尽量抬，但不保证），照着 tick 发就会偏慢，
	一路慢下去声音就是拖的、越拖越多。这里改成**对着墙上时间算**该发第几包了：
	醒了就问「从开始到现在本该发多少包」，补上差额（一次最多补 4 包，
	免得长时间挂起之后一口气爆发）。tick 只是「多久来看一眼」，不是节拍本身。
*/
func (a *audioSender) pump() {
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	start := time.Now()
	var sent int64
	for {
		select {
		case <-a.quit:
			return
		case <-tick.C:
			due := int64(time.Since(start)/pktEvery) + 1
			for i := 0; i < 4 && sent < due; i++ {
				if err := a.sendOne(); err != nil {
					log.Printf("发送音频失败：%v", err)
					return
				}
				sent++
			}
			if sent < due {
				/* 落后太多（机器睡了、被挂起了）就不追了，直接对齐到当前时刻 ——
				   补发一堆过去的声音没有任何意义。 */
				sent = due
			}
		}
	}
}

/*
sendOne 发一包。缓冲里有够 320 字节就用它，不够（或被静音了）就发静音：

	两种情况在下面合成同一个分支 —— pkt 的后 320 字节建出来就是全零。
*/
func (a *audioSender) sendOne() error {
	pkt := make([]byte, 20+pcmBytes)

	a.mu.Lock()
	if !a.muted && len(a.buf) >= pcmBytes {
		copy(pkt[20:], a.buf[:pcmBytes])
		a.buf = a.buf[pcmBytes:]
		if len(a.buf) == 0 {
			a.buf = nil
		}
	}
	seq, ctr := a.seq, a.counter
	a.seq++
	a.counter++
	a.mu.Unlock()

	putHeader(pkt[:20], a.srcIP, a.srcPort, seq, ctr, pcmBytes)
	_, err := a.conn.Write(pkt)
	return err
}

func putHeader(h []byte, srcIP [4]byte, srcPort uint16, seq uint32, ctr uint16, n int) {
	copy(h[0:4], srcIP[:])
	binary.BigEndian.PutUint16(h[4:6], srcPort)
	h[6], h[7] = 0x04, 0x22
	binary.BigEndian.PutUint32(h[8:12], seq)
	h[12], h[13], h[14], h[15] = 0x7f, 0xff, 0xff, 0xff
	binary.BigEndian.PutUint16(h[16:18], uint16(n))
	binary.BigEndian.PutUint16(h[18:20], ctr)
}
