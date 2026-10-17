/* ws —— 够用就好的 RFC 6455
   play-agent · 广播控制台

   只实现这件事真正用到的那部分：握手、文本帧、二进制帧、ping / pong / close，
   外加分片重组（浏览器什么时候分片不由我们定）。**不实现** permessage-deflate ——
   握手时压根不协商它，所以它不会被启用；也不做 UTF-8 校验，发文本的两头
   （浏览器、厂商那套客户端）都不是攻击者。

   同一个类型两边都用：
     · 对网页是**服务端** —— 我们发出去的帧不掩码，浏览器发来的必掩码；
     · 对打铃服务器是**客户端** —— 我们发出去的帧必掩码，服务器发来的不掩码。
   区别只有 wsConn.mask 一个字段（RFC 6455 §5.3：客户端必须掩码，服务端必须不掩码）。
*/

package main

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"time"
)

/* 帧类型。0x0 是「接着上一帧」，0x8 以上是控制帧。 */
const (
	opCont  = 0x0
	opText  = 0x1
	opBin   = 0x2
	opClose = 0x8
	opPing  = 0x9
	opPong  = 0xA
)

/* RFC 6455 §1.3 那个固定串，用来算 Sec-WebSocket-Accept。 */
const wsMagic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

/*
单帧上限。我们两边收的都只有几百字节（JSON 命令、320 字节 PCM），

	一兆是给「有人往这儿灌东西」留个止损位，不是性能参数。
*/
const wsMaxFrame = 1 << 20

type wsConn struct {
	c    net.Conn
	r    *bufio.Reader /* 必须和握手用的是同一个 —— 见 wsUpgrade 里那段 */
	mask bool          /* 我们发出去的帧要不要掩码 */

	wmu sync.Mutex /* 写是并发的（泵在发 PCM，keepalive 在发 ping），串起来 */
}

/*
══ 读 ═══════════════════════════════════════════════════════════════

	readMessage 交出一条完整的消息（分片已经拼好），控制帧在内部消化掉：
	ping 当场回 pong，pong 忽略，close 返回 io.EOF。

	**控制帧可以插在一条分片消息中间**（RFC 6455 §5.4），所以这个循环不能
	假设「读完一帧就是一条消息」。
*/
func (w *wsConn) readMessage() (op byte, payload []byte, err error) {
	var buf []byte
	started := false
	for {
		fin, opcode, chunk, err := w.readFrame()
		if err != nil {
			return 0, nil, err
		}
		switch opcode {
		case opClose:
			return 0, nil, io.EOF
		case opPing:
			/* 回 pong 要带上原样的载荷（§5.5.3）。回错方向就会死循环：
			   我们发出去的是「不掩码」还是「掩码」看 w.mask，不在这儿判断。 */
			_ = w.writeFrame(opPong, chunk, true)
			continue
		case opPong:
			continue
		case opCont:
			if !started {
				return 0, nil, errors.New("收到无起始帧的续帧")
			}
			buf = append(buf, chunk...)
		case opText, opBin:
			if started {
				return 0, nil, errors.New("上一条分片消息尚未拼装完成")
			}
			op, started = opcode, true
			buf = append(buf, chunk...)
		default:
			return 0, nil, fmt.Errorf("未知帧类型 %#x", opcode)
		}
		if len(buf) > wsMaxFrame {
			return 0, nil, errors.New("消息过大")
		}
		if fin && started {
			return op, buf, nil
		}
	}
}

/* readFrame 读**一帧**，掩码就地解掉。 */
func (w *wsConn) readFrame() (fin bool, op byte, payload []byte, err error) {
	var h [2]byte
	if _, err = io.ReadFull(w.r, h[:]); err != nil {
		return
	}
	fin = h[0]&0x80 != 0
	if h[0]&0x70 != 0 {
		err = errors.New("RSV 位非 0（未协商扩展）")
		return
	}
	op = h[0] & 0x0f
	masked := h[1]&0x80 != 0
	n := int64(h[1] & 0x7f)

	switch n {
	case 126:
		var e [2]byte
		if _, err = io.ReadFull(w.r, e[:]); err != nil {
			return
		}
		n = int64(binary.BigEndian.Uint16(e[:]))
	case 127:
		var e [8]byte
		if _, err = io.ReadFull(w.r, e[:]); err != nil {
			return
		}
		u := binary.BigEndian.Uint64(e[:])
		if u > wsMaxFrame {
			err = fmt.Errorf("帧过大：%d 字节", u)
			return
		}
		n = int64(u)
	}
	if n > wsMaxFrame {
		err = fmt.Errorf("帧过大：%d 字节", n)
		return
	}
	/* 控制帧不许分片、载荷不许超过 125 字节（§5.5）。 */
	if op >= 0x8 && (!fin || n > 125) {
		err = errors.New("控制帧不合法")
		return
	}

	var key [4]byte
	if masked {
		if _, err = io.ReadFull(w.r, key[:]); err != nil {
			return
		}
	}
	payload = make([]byte, n)
	if _, err = io.ReadFull(w.r, payload); err != nil {
		return
	}
	if masked {
		for i := range payload {
			payload[i] ^= key[i&3]
		}
	}
	return
}

/* ══ 写 ═══════════════════════════════════════════════════════════════ */
func (w *wsConn) writeFrame(op byte, payload []byte, fin bool) error {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	return w.writeFrameLocked(op, payload, fin)
}

func (w *wsConn) writeFrameLocked(op byte, payload []byte, fin bool) error {
	n := len(payload)
	/* 头最长 14 字节（2 + 8 长度 + 4 掩码），一次 write 发完一帧 ——
	   分两次写虽然也对，但一次更省事，也不给「半个帧卡在缓冲里」留机会。 */
	out := make([]byte, 0, 14+n)
	b0 := op
	if fin {
		b0 |= 0x80
	}
	out = append(out, b0)

	maskBit := byte(0)
	if w.mask {
		maskBit = 0x80
	}
	switch {
	case n < 126:
		out = append(out, maskBit|byte(n))
	case n <= 0xffff:
		out = append(out, maskBit|126, byte(n>>8), byte(n))
	default:
		out = append(out, maskBit|127)
		var e [8]byte
		binary.BigEndian.PutUint64(e[:], uint64(n))
		out = append(out, e[:]...)
	}

	if w.mask {
		var key [4]byte
		if _, err := rand.Read(key[:]); err != nil {
			return err
		}
		out = append(out, key[:]...)
		for i := 0; i < n; i++ {
			out = append(out, payload[i]^key[i&3])
		}
	} else {
		out = append(out, payload...)
	}
	_, err := w.c.Write(out)
	return err
}

func (w *wsConn) writeText(s string) error { return w.writeFrame(opText, []byte(s), true) }

/*
Close 道个别再关。对方已经没了的话（EPIPE 之类）也照样关 —— 这一步的

	意义只是「尽量让对面知道」，不是「必须送到」。
*/
func (w *wsConn) Close() error {
	_ = w.writeFrame(opClose, nil, true)
	return w.c.Close()
}

/*
══ 握手 · 服务端（对网页）═════════════════════════════════════════

	originOK 拿到的是 Origin 头的原值（浏览器必带；脚本客户端可能不带，那时是空串）
	和这条请求的 Host 头（网页是拿哪个地址连过来的）。返回 false 就拒绝 —— 代理
	可以绑在局域网地址上，**网里任何一个网页**都能往这个口连，不查 Origin 等于谁都
	能借它往学校广播喊话。判定规则见 main.go 的 originAllowed。
*/
func wsUpgrade(c net.Conn, originOK func(origin, host string) bool) (*wsConn, error) {
	/* **这个 bufio.Reader 要一直用下去。** 握手那几行是用它读的，
	   读完最后一个空行时它可能已经把第一帧的头几个字节也读进缓冲区了 ——
	   后面换成直接读 c 的话，那几个字节就丢了，表现是「连上就卡住」。 */
	br := bufio.NewReader(c)
	tp := textproto.NewReader(br)

	if err := c.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, err
	}
	line, err := tp.ReadLine()
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "GET ") {
		return nil, fmt.Errorf("不是 WebSocket 握手请求：%q", line)
	}
	h, err := tp.ReadMIMEHeader()
	if err != nil {
		return nil, err
	}
	_ = c.SetReadDeadline(time.Time{})

	if !strings.EqualFold(h.Get("Upgrade"), "websocket") ||
		!strings.Contains(strings.ToLower(h.Get("Connection")), "upgrade") {
		return nil, errors.New("不是 WebSocket 升级请求")
	}
	if h.Get("Sec-WebSocket-Version") != "13" {
		return nil, fmt.Errorf("不支持的 WebSocket 版本：%q", h.Get("Sec-WebSocket-Version"))
	}
	key := h.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("缺少 Sec-WebSocket-Key")
	}
	origin := h.Get("Origin")
	if originOK != nil && !originOK(origin, h.Get("Host")) {
		/* 带上原样的 Origin 回一句，让人一眼知道该往 --origin 里填什么。 */
		_, _ = io.WriteString(c,
			"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return nil, fmt.Errorf("Origin 不在白名单内：%q", origin)
	}

	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + wsAccept(key) + "\r\n\r\n"
	if _, err := io.WriteString(c, resp); err != nil {
		return nil, err
	}
	return &wsConn{c: c, r: br, mask: false}, nil
}

/*
══ 握手 · 客户端（对打铃服务器）═══════════════════════════════════

	照抓包里那次成功的握手逐字来：路径 `/`，带一行**值为空**的
	Sec-WebSocket-Protocol（厂商客户端就发了它，服务器没理它也没回它）。
*/
func wsDial(rawURL string) (*wsConn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" {
		return nil, fmt.Errorf("仅支持 ws://（打铃服务器未启用 TLS）：%q", rawURL)
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "80")
	}
	path := u.Path
	if path == "" {
		path = "/"
	}
	if u.RawQuery != "" {
		path += "?" + u.RawQuery
	}

	c, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		return nil, err
	}
	var kb [16]byte
	if _, err := rand.Read(kb[:]); err != nil {
		_ = c.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(kb[:])

	req := "GET " + path + " HTTP/1.1\r\n" +
		"Sec-WebSocket-Protocol: \r\n" +
		"Connection: Upgrade\r\n" +
		"Upgrade: websocket\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Host: " + host + "\r\n\r\n"
	if _, err := io.WriteString(c, req); err != nil {
		_ = c.Close()
		return nil, err
	}

	br := bufio.NewReader(c)
	tp := textproto.NewReader(br)
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	status, err := tp.ReadLine()
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	h, err := tp.ReadMIMEHeader()
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	_ = c.SetReadDeadline(time.Time{})

	if !strings.Contains(status, "101") {
		_ = c.Close()
		return nil, fmt.Errorf("服务器未同意升级：%q", status)
	}
	if got, want := h.Get("Sec-WebSocket-Accept"), wsAccept(key); got != want {
		_ = c.Close()
		return nil, errors.New("Sec-WebSocket-Accept 不匹配")
	}
	return &wsConn{c: c, r: br, mask: true}, nil
}

func wsAccept(key string) string {
	h := sha1.New()
	_, _ = io.WriteString(h, key+wsMagic)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
