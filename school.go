/* school —— 对打铃服务器那半边
   play-agent · 广播控制台

   一条长连接，报文是 JSON 文本帧，外面套同一层壳：

     {actioncode, company:"BL", data:{…}, result:0, return_message:"", token:<JWT>}

   **代理自己不登录。** 面板那条终端状态连接（js/15-status.js）从来没发过
   c2ls_user_login —— 它把 REST 登录拿到的 JWT 直接塞在 token 字段里就通了。
   所以这里也一样：JWT 由网页在 call 命令里递过来，代理不碰口令、不落盘。

   一次呼叫是三条报文，顺序不能反：
     1. collect_pcm   开一路采集任务，HashKey 指着我们那条 UDP 套接字
     2. stop_task     停掉它
     3. unsubscribe_remote_task   退订，把这条任务从服务器上摘干净
*/

package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	actCollect = "c2ls_local_soundcard_collect_pcm"
	actStop    = "c2ls_stop_task"
	actUnsub   = "c2ls_unsubscribe_remote_task"

	ackCollect = "ls2c_local_soundcard_collect_pcm"
)

/*
keepAlive 是我们主动发 ping 的间隔。厂商客户端抓包里 ping 得很勤

	（几百毫秒一个），我们不必那么密，但**不能不发**：中间隔着 NAT 和一堆
	交换机，一条几分钟不说话的 TCP 会被悄悄掐掉，而那时候正好是有人在讲话。
*/
const keepAlive = 2 * time.Second

type envelope struct {
	Actioncode    string          `json:"actioncode"`
	Company       string          `json:"company"`
	Data          json.RawMessage `json:"data"`
	Result        int             `json:"result"`
	ReturnMessage string          `json:"return_message"`
	Token         string          `json:"token"`
}

type school struct {
	mu    sync.Mutex
	conn  *wsConn
	url   string
	token string
	recv  chan envelope /* 每条收进来的报文；连接断了就关掉 */

	/* 连接断掉时喊一声。**必须喊**：不喊的话这边还在按 100 包/秒往一个
	   已经没人收的地址发音频，操作员那头看着是「正在讲话」，其实早断了。 */
	onDrop func()
}

/*
ensure 按需连上，并且**认地址和令牌**：换了服务器或换了 JWT 就重连一条，

	没换就接着用。省掉的是「每次呼叫都重新握手 + 重新登录」这一趟。
*/
func (s *school) ensure(rawURL, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil && s.url == rawURL && s.token == token {
		return nil
	}
	s.dropLocked()

	c, err := wsDial(rawURL)
	if err != nil {
		return err
	}
	recv := make(chan envelope, 32)
	s.conn, s.url, s.token, s.recv = c, rawURL, token, recv
	log.Printf("已连接打铃服务器 %s", rawURL)
	go s.readLoop(c, recv)
	go s.pingLoop(c)
	return nil
}

func (s *school) dropLocked() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.recv = nil
}

func (s *school) readLoop(c *wsConn, recv chan envelope) {
	defer func() {
		close(recv)
		if s.onDrop != nil {
			s.onDrop()
		}
	}()
	for {
		op, payload, err := c.readMessage()
		if err != nil {
			log.Printf("打铃服务器连接已断开：%v", err)
			return
		}
		if op != opText {
			continue /* 服务器不发二进制，真来了也只当没看见 */
		}
		var e envelope
		if err := json.Unmarshal(payload, &e); err != nil {
			continue
		}
		select {
		case recv <- e:
		default:
			/* 没人等着看的推送（在线状态那类）攒满就丢 —— 丢掉的是别人的事，
			   不是我们这条任务的应答。 */
		}
	}
}

func (s *school) pingLoop(c *wsConn) {
	t := time.NewTicker(keepAlive)
	defer t.Stop()
	for range t.C {
		var b [4]byte
		_, _ = rand.Read(b[:])
		if err := c.writeFrame(opPing, b[:], true); err != nil {
			return
		}
	}
}

/* send 发一条。data 是已经 marshal 好的 JSON。 */
func (s *school) send(actioncode, token string, data interface{}) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	env := envelope{
		Actioncode: actioncode,
		Company:    "BL",
		Data:       body,
		Result:     0,
		Token:      token,
	}
	out, err := json.Marshal(env)
	if err != nil {
		return err
	}
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if c == nil {
		return fmt.Errorf("尚未连接打铃服务器")
	}
	if err := c.writeText(string(out)); err != nil {
		s.mu.Lock()
		s.dropLocked()
		s.mu.Unlock()
		return err
	}
	return nil
}

/*
startCollect 发 collect_pcm 并等应答。**等到应答才算接通** —— 面板在

	call 回来之后就让操作员开口了，报早了等于让人对着一条根本没建起来的
	任务说话。

	★ 这里是整段最没底的一处：厂商客户端在这之前发过 c2ls_user_login，
	  我们不发，指着「token 字段里的 JWT 一样认」。状态那条连接上是认的
	  （面板就在这么用），但 collect_pcm 是不是也认，得拿真服务器试一次。
*/
func (s *school) startCollect(token string, d collectData, timeout time.Duration) error {
	s.mu.Lock()
	recv := s.recv
	s.mu.Unlock()
	if recv == nil {
		return fmt.Errorf("尚未连接打铃服务器")
	}
	if err := s.send(actCollect, token, d); err != nil {
		return err
	}

	deadline := time.After(timeout)
	for {
		select {
		case e, ok := <-recv:
			if !ok {
				return fmt.Errorf("打铃服务器连接已断开")
			}
			if e.Actioncode != ackCollect {
				continue
			}
			if e.Result != 200 {
				if e.ReturnMessage != "" {
					return fmt.Errorf("服务器拒绝该任务：%s", e.ReturnMessage)
				}
				return fmt.Errorf("服务器拒绝该任务（result=%d）", e.Result)
			}
			return nil
		case <-deadline:
			return fmt.Errorf("服务器未在 %s 内应答", timeout)
		}
	}
}

/*
stop 收摊：两条报文各发一次，**不等应答**。挂断这件事必须立刻办成 ——

	人已经松手了，这时候还卡着等服务器回话，只会让「挂了但还在响」多持续几秒。
	真失败了下一次呼叫会重新登录/重连，不会攒着。
*/
func (s *school) stop(token, taskID string) {
	d := map[string]string{"TaskID": taskID}
	if err := s.send(actStop, token, d); err != nil {
		log.Printf("stop_task 发送失败：%v", err)
	}
	if err := s.send(actUnsub, token, d); err != nil {
		log.Printf("unsubscribe_remote_task 发送失败：%v", err)
	}
}

func (s *school) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropLocked()
}

/*
══ collect_pcm 的 data ═══════════════════════════════════════════

	字段名和值全部照抓包。EndPointsAdditionalProp 是个空对象，但**不能省** ——
	抓包里它在，缺字段和空对象在服务端可能不是一回事。
*/
type collectData struct {
	AudioBlockSize          int               `json:"AudioBlockSize"`
	AudioQuality            int               `json:"AudioQuality"`
	EndPointsAdditionalProp map[string]string `json:"EndPointsAdditionalProp"`
	EndPointsList           string            `json:"EndPointsList"`
	HashKey                 string            `json:"HashKey"`
	TaskID                  string            `json:"TaskID"`
	TaskName                string            `json:"TaskName"`
	TaskPriority            int               `json:"TaskPriority"`
	Volume                  int               `json:"Volume"`
}

func newCollectData(endpoints []string, hashKey string) collectData {
	return collectData{
		AudioBlockSize:          pcmBytes,
		AudioQuality:            16,
		EndPointsAdditionalProp: map[string]string{},
		EndPointsList:           strings.Join(endpoints, ","),
		HashKey:                 hashKey,
		TaskID:                  newTaskID(),
		TaskName:                "声卡_" + time.Now().Format("060102150405"),
		TaskPriority:            50,
		/* Volume 是服务端给这路流用的播放音量，0–100（和面板里任务、设备的
		   音量同一把尺）。原来照抓包写了 5 —— 抓包那台官方客户端当时的音量
		   正好是 5，不是协议常量，结果推出去的流只有 5%，喇叭上几乎听不见
		   （2026-10-17 用户实测：对讲和在线播放都小得离谱，iOS 和 Go 两版同病，
		   说明问题在 collect_pcm 这条共用的报文里）。用户定的是推满：100。 */
		Volume: 100,
	}
}

/* newTaskID 照厂商的形状：{32 位大写十六进制}，带花括号。 */
func newTaskID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "{" + strings.ToUpper(hex.EncodeToString(b[:])) + "}"
}
