/* play-agent —— 播放代理
   广播控制台 · 替代厂商后台的打铃配置面板

   它只做一件事：**把网页里抓到的麦克风 PCM 转交给打铃服务器。**

   ── 为什么非要有这么个东西 ─────────────────────────────────────────
   浏览器里没有原始套接字，对讲那条音频流是 UDP（打铃服务器上一个固定端口），
   网页发不出去；另外控制通道和音频口都是明文的，面板挂在 https 上的话连
   `ws://` 也出不去（混合内容拦截）。所以这一跳得有个本地的进程替网页走完。

   ── 装在哪台机器上 ──────────────────────────────────────────────
   **主要装法：装在打铃服务器那台机器上**（和面板同一个主机名），操作员拿手机、
   拿办公室电脑打开网页就行 —— 手机跑不了代理，代理不放服务器上就没地方放。
   也可以装在操作员自己电脑上（面板设置页填 127.0.0.1）。
   **默认绑通配地址**（0.0.0.0:8181 / 0.0.0.0:8182）：代理多半和面板一起装在
   打铃服务器上，操作员拿手机、办公室电脑打开网页连过来，只有绑通配地址才连得着。
   只想给本机用时显式 `--listen 127.0.0.1:8181`，见 main 里打「已绑定」那条日志
   的地方。绑哪儿由 --listen 说了算，三种都支持。

   ── 它做的事 ───────────────────────────────────────────────────
     网页 ──ws://<代理那台机器>:8181──▶ 本进程 ──ws://…:51330──▶ 打铃服务器（控制）
                                              └──udp──▶ …:51340     （音频）

   四条命令，和面板 js/75-view-live.js 里那套一问一答对齐：
     {cmd:"call",   data:{ws, udp, token, endpoints:["id"…], names:[…]}}
                    ↑ endpoints 是**字符串**数组（终端 ID 转成字符串再发）
     {cmd:"mute",   data:{on:true|false}}
     {cmd:"hangup", data:{}}
     二进制帧 = 麦克风 PCM（16k / 16 位 / 小端 / 单声道），只在 call 和 hangup 之间
   回 {id, ok:true} 或 {id, ok:false, message:"…"}。
   **代理不主动往网页推消息** —— 和打铃服务器的连接断在半路时它会自己收摊
   （停音频、stop_task），但面板那边要等下一次命令才知道，这是目前留着的一个缺口。

   **服务器地址、JWT、目标终端全都是网页递过来的**，代理本身零配置 ——
   没有配置文件就没有要保密的文件，口令也不在这台机器上落盘。

   用法：
     play-agent                         # 默认绑 0.0.0.0:8181（局域网里别的机器也连得过来）
     play-agent --listen 127.0.0.1:8181 # 只给本机用
     play-agent --origin http://…       # 面板在**别的**主机名上时，把它的源加进来
                                        # （面板和代理同主机名的话不用填，见 originAllowed）
     play-agent --web-dir D:\panel      # 顺带把网页目录伺服出去（默认 0.0.0.0:8182）
     play-agent --web-dir D:\panel --web-listen 0.0.0.0:82
                                        # 换个端口／只开给本机时用它
*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

/*
开局那张抬头上的名字和版本号（见 main 里打印它的那一段）。

	版本号改这儿就行 —— 别处没有第二个地方认它。
*/
const (
	appName    = "ITC-PlayAgent"
	appVersion = "1.0.0"
)

/*
等服务器认下 collect_pcm 的上限。面板那边等的是 8 秒（liveRequest 的超时），

	这里得比它短 —— 先于它报错，操作员看到的才是「服务器说什么」，
	而不是一句笼统的「播放代理没有回应」。
*/
const collectTimeout = 5 * time.Second

type agent struct {
	mu    sync.Mutex
	sch   school
	audio *audioSender
	task  string
	token string
	live  bool
}

func main() {
	listen := flag.String("listen", "0.0.0.0:8181", "监听地址（默认 0.0.0.0:8181，局域网里别的机器也能连；只想本机连填 127.0.0.1:8181）")
	origin := flag.String("origin", "", "额外允许的 Origin，逗号分隔（面板在别的主机名上时才要填）")
	webDir := flag.String("web-dir", "", "网页目录。给了它就顺带开静态托管，不给就不开")
	webListen := flag.String("web-listen", "0.0.0.0:8182", "静态托管的监听地址（默认 0.0.0.0:8182，只在给了 --web-dir 时才有意义）")
	/* Go 的 flag 包把 `-x` 和 `--x` 当成同一个写法，两种都收；但我们对外一律写
	   两个横杠（用户定的），所以这里把 PrintDefaults 吐出来的单横杠补成双横杠 ——
	   不然 `--help` 里印的是一副样子，README 里写的是另一副。 */
	flag.Usage = func() {
		out := flag.CommandLine.Output()
		fmt.Fprintf(out, "用法：%s [选项]\n\n选项：\n", filepath.Base(os.Args[0]))
		var b strings.Builder
		flag.CommandLine.SetOutput(&b)
		flag.PrintDefaults()
		flag.CommandLine.SetOutput(out)
		for _, line := range strings.SplitAfter(b.String(), "\n") {
			if strings.HasPrefix(line, "  -") {
				line = "  -" + line[2:]
			}
			fmt.Fprint(out, line)
		}
	}
	flag.Parse()

	extra := splitList(*origin)

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("监听 %s 失败：%v", *listen, err)
	}
	/* ── 静态托管（可选）────────────────────────────────────────────
	   给了 --web-dir 就开。**不再另加一个开关**（用户定的）：一个参数同时说清
	   「开不开」和「伺哪个目录」，少一个能和它对不上的状态。

	   **监听建在牌子前面、serve 留在牌子后面**：牌子上要多印一段
	   「HTTP listening on:」（用户定的），而端口是操作系统分的，不先把监听建
	   出来就不知道写哪个号。卡在这儿的两种错都直接退出，不带着半条命往下跑 ——
	   端口被占（比如 dufs 还占着 82）的时候，人打开网页看到的是一个旧面板，
	   却以为新的已经起来了，那种错最难查。 */
	var webLn net.Listener
	if *webDir != "" {
		webLn, err = openWeb(*webListen, *webDir)
		if err != nil {
			log.Fatal(err)
		}
	}

	/* ── 开局这张抬头 ────────────────────────────────────────────────
	   名字 + 版本号、一个空行、「Listening on:」、下面每条绑定的地址各一行
	   （前面空两格）；开了静态托管就再空一行、接着印「HTTP listening on:」，
	   形状同上；最后空一行，然后才开始出日志 —— 顺序和间距都是定死的。
	   **不走 log 包**：它给每一行都套一个时间戳前缀，而这块牌子不是一条事件
	   ——「2009/11/23 01:23:23 ITC-PlayAgent v1.0.0」很难看。
	   写的是 stderr：和 log 包同一个流，所以这几行和紧跟在后面的日志保证按顺序
	   出现（分到两个流的话，谁先谁后看终端怎么缓冲，重定向到文件时还会被拆开）。 */
	fmt.Fprintf(os.Stderr, "%s v%s\n\n", appName, appVersion)
	fmt.Fprintln(os.Stderr, "Listening on:")
	for _, u := range localURLs(ln.Addr(), "ws") {
		fmt.Fprintf(os.Stderr, "  %s\n", u)
	}
	if webLn != nil {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "HTTP listening on:")
		for _, u := range localURLs(webLn.Addr(), "http") {
			fmt.Fprintf(os.Stderr, "  %s\n", u)
		}
	}
	fmt.Fprintln(os.Stderr)

	/* 抬头下面第一条日志：绑在哪儿。绑哪儿由 --listen 说了算，三种都合法：
	     · 0.0.0.0（**默认**）—— 代理装在打铃服务器那台机器上，操作员拿手机或
	       别的电脑打开网页连过来。手机跑不了代理，而面板本身就在服务器上，
	       代理不放那儿就没地方放，所以真部署的时候多半是这一种。
	     · 回环 —— 代理和操作员在同一台机器上，别人连不着。
	     · 某个具体的局域网地址 —— 同上，只是绑死在一张网卡上。
	   **默认绑通配地址（用户定的）**：这个口只要开着，网里能连上来的机器都能借它
	   往学校广播喊话（真喊出去还得有一张有效的 JWT —— 代理自己不存口令、也没有
	   登录这一步，那道门在打铃服务器上、不在这儿）。只想本机用时显式
	   `--listen 127.0.0.1:8181`，下面那句提示就是告诉人怎么关回去。

	   两句话分开（用户定的）：绑回环时报「怎么开给局域网」；绑局域网时那句报的
	   已经是既成事实，后面不用再交代什么。行首不带任何符号。 */
	if isLoopback(ln.Addr()) {
		log.Printf("已绑定 %s，如需从局域网访问，请使用 --listen 参数重新运行", ln.Addr())
	} else {
		log.Printf("已绑定 %s", ln.Addr())
	}
	if len(extra) > 0 {
		log.Printf("额外允许的 Origin：%s", strings.Join(extra, ", "))
	}

	/* ── 静态托管真的开始伺服 ───────────────────────────────────────
	   监听上面就建好了（牌子要它），这儿只是把它交出去。 */
	if webLn != nil {
		serveWeb(webLn, *webDir)
	}

	ag := &agent{}
	/* 和打铃服务器那条连接断了就当场收摊。**不是防御性代码**：断在半路时
	   不主动收，这边会一直往一个没人收的地址发音频，而操作员屏幕上写着
	   「正在讲话」—— 一个人对着空气喊了几分钟才发现，这是最坏的一种失败。 */
	ag.sch.onDrop = func() { _ = ag.hangup("与打铃服务器的连接已断开") }

	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("接受连接失败：%v", err)
			continue
		}
		go ag.serve(c, extra)
	}
}

/* serve 一条来自网页的连接。 */
func (ag *agent) serve(c net.Conn, extra []string) {
	ws, err := wsUpgrade(c, func(o, host string) bool { return originAllowed(o, host, extra) })
	if err != nil {
		log.Printf("握手失败（%s）：%v", c.RemoteAddr(), err)
		_ = c.Close()
		return
	}
	log.Printf("客户端已连接（%s）", c.RemoteAddr())

	for {
		op, payload, err := ws.readMessage()
		if err != nil {
			break
		}
		switch op {
		case opText:
			ag.command(ws, payload)
		case opBin:
			/* 二进制帧就是 PCM，不解析、不排队，直接喂给泵。 */
			ag.pushPCM(payload)
		}
	}

	ag.hangup("客户端断开")
	_ = ws.Close()
	_ = c.Close()
	log.Printf("客户端已断开（%s）", c.RemoteAddr())
}

/* ══ 三条命令 ═══════════════════════════════════════════════════════ */
func (ag *agent) command(ws *wsConn, payload []byte) {
	var m struct {
		Cmd  string          `json:"cmd"`
		ID   int             `json:"id"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &m); err != nil {
		log.Printf("命令解析失败：%v", err)
		return
	}

	var err error
	switch m.Cmd {
	case "call":
		err = ag.call(m.Data)
	case "mute":
		err = ag.mute(m.Data)
	case "hangup":
		err = ag.hangup("操作员挂断")
	default:
		err = fmt.Errorf("未知命令 %q", m.Cmd)
	}
	if err != nil {
		log.Printf("%s 执行失败：%v", m.Cmd, err)
	}
	reply(ws, m.ID, err)
}

func (ag *agent) call(raw json.RawMessage) error {
	var d struct {
		WS        string   `json:"ws"`
		UDP       string   `json:"udp"`
		Token     string   `json:"token"`
		Endpoints []string `json:"endpoints"`
		Names     []string `json:"names"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return fmt.Errorf("call 参数无效：%v", err)
	}
	if d.WS == "" || d.UDP == "" {
		return fmt.Errorf("客户端未提供对讲地址")
	}
	if d.Token == "" {
		return fmt.Errorf("客户端未发送令牌")
	}
	if len(d.Endpoints) == 0 {
		return fmt.Errorf("未指定目标终端")
	}

	/* 上一通还挂着就先收掉。这不是防御性代码，是真实会发生的：面板那边
	   「呼叫」按下去失败一次、人再按一次，第二发就到了。 */
	ag.hangup("新呼叫已取代上一通")

	if err := ag.sch.ensure(d.WS, d.Token); err != nil {
		return err
	}
	/* ★ 顺序是死的：**先建套接字、问出 ip:port，再拿它当 HashKey 去发
	   collect_pcm**。反过来的话服务器认领的是另一条流，这边发多少都是白搭。 */
	a, err := newAudioSender(d.UDP)
	if err != nil {
		return err
	}
	data := newCollectData(d.Endpoints, a.hashKey)
	if err := ag.sch.startCollect(d.Token, data, collectTimeout); err != nil {
		a.stop()
		return err
	}

	ag.mu.Lock()
	ag.audio, ag.task, ag.token, ag.live = a, data.TaskID, d.Token, true
	ag.mu.Unlock()

	log.Printf("开始对讲：%d 个终端（%s）发往 %s，本机音频口 %s",
		len(d.Endpoints), strings.Join(d.Names, "、"), d.UDP, a.hashKey)
	return nil
}

func (ag *agent) mute(raw json.RawMessage) error {
	var d struct {
		On bool `json:"on"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return err
	}
	ag.mu.Lock()
	a := ag.audio
	ag.mu.Unlock()
	if a == nil {
		return fmt.Errorf("当前无进行中的对讲")
	}
	a.setMuted(d.On)
	return nil
}

/*
hangup 收摊。**没在对讲时调它是正常的**（网页断开、新呼叫顶替、

	面板那边连着点两下），所以不报错，安静地什么都不做。
*/
func (ag *agent) hangup(why string) error {
	ag.mu.Lock()
	a, task, token, live := ag.audio, ag.task, ag.token, ag.live
	ag.audio, ag.task, ag.live = nil, "", false
	ag.mu.Unlock()
	if !live {
		return nil
	}
	a.stop()
	ag.sch.stop(token, task)
	log.Printf("对讲结束（%s）", why)
	return nil
}

func (ag *agent) pushPCM(p []byte) {
	ag.mu.Lock()
	a := ag.audio
	ag.mu.Unlock()
	if a != nil {
		a.push(p)
	}
}

/* ══ 回话 ═══════════════════════════════════════════════════════════ */
func reply(ws *wsConn, id int, err error) {
	m := map[string]interface{}{"id": id, "ok": err == nil}
	if err != nil {
		m["message"] = err.Error()
	}
	b, _ := json.Marshal(m)
	_ = ws.writeText(string(b))
}

/*
══ Origin 白名单 ══════════════════════════════════════════════════

	代理可以绑在局域网地址上，那个口**网里任何一个网页**都能连 —— 不查 Origin，
	等于随便哪个广告页面都能借它往学校广播喊话。放行三类：

	 1. 面板和代理**同一个主机名**（Origin 的主机 == 请求的 Host）——
	    这是主要的装法：面板由打铃服务器上的 dufs 提供，代理也跑在那台机器上，
	    于是 `http://192.168.1.100:81` 里的页面连 `ws://192.168.1.100:8181`，
	    两边主机名天然一致。**不用配置**。端口不参与比较（一个 81 一个 8181）。
	 2. 本机那几种（localhost / 127.0.0.1 / ::1）和 `null`（file:// 打开的面板）——
	    代理装在操作员自己电脑上时走这条。
	 3. --origin 里显式列出来的。

	剩下的按域名比，**不是按 IP 白名单**：别处某个网页借这个口喊话时，它的 Origin
	是自己那个域名，和它是拿哪个地址连过来的对不上，直接拒。
*/
func originAllowed(origin, host string, extra []string) bool {
	/* 不带 Origin 的不是浏览器（本机脚本、curl 之类）—— 它本来就能干任何事，
	   拦它没有意义，只会让自测变麻烦。 */
	if origin == "" {
		return true
	}
	for _, e := range extra {
		if strings.EqualFold(e, origin) {
			return true
		}
	}
	/* file:// 打开的面板带的是 "null"。放行它就把「沙箱里的 iframe」也放进来了，
	   但在只绑回环的前提下这个代价可以接受 —— 换成不放行的话，本机双击
	   index.html 就直接用不了。 */
	if origin == "null" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	oh := strings.ToLower(u.Hostname())
	switch oh {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	/* 上面第 1 类：Origin 的主机和网页连过来的那个主机名是同一个。
	   Host 头形如 "192.168.1.100:8181" 或 "[::1]:8181"，也可能不带端口。 */
	hh := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		hh = h
	}
	/* Host 为空（老客户端不带这个头）就只认上面那几条，不拿空串去比。 */
	return hh != "" && strings.EqualFold(oh, hh)
}

/*
══ 静态托管 ═══════════════════════════════════════════════════════

	「一个 exe 顶掉 dufs」这件事的全部实现：net/http 加一个 FileServer，二十来行，
	零第三方依赖（和这个程序的其他部分一样）。
	**目录是原样伺服的** —— 里面有什么就能下什么。所以 --web-dir 指的那个目录得是
	干净的：面板的 config.json 里是真实终端名和考试安排，不要连它一起端到局域网
	上去（拷到服务器上的那份只放 index.html + js/ + css/ 三样）。
	和打铃服务器那条 ws 同主机名（端口不同不碍事，originAllowed 比的是主机名），
	所以面板改由这儿伺服之后，--origin 一般照样不用填。

	**分两步，是有意的**：openWeb 只负责建监听（错要当场报出来），serveWeb 才
	真开始伺服。中间那一步是抬头牌子上的「HTTP listening on:」—— 不先把监听建
	出来，就不知道端口落在哪儿，那行字写不出来（见 main 里的顺序）。
*/
func openWeb(addr, dir string) (net.Listener, error) {
	st, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("网页目录打不开（%s）：%v", dir, err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("网页目录不是目录（%s）", dir)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("静态托管监听 %s 失败：%v", addr, err)
	}
	return ln, nil
}

func serveWeb(ln net.Listener, dir string) {
	log.Printf("静态托管已启动：%s，目录 %s", ln.Addr(), dir)
	go func() { _ = http.Serve(ln, http.FileServer(http.Dir(dir))) }()
}

/*
localURLs 列出「网页实际连得上」的地址，只用来打日志。scheme 是 "ws" 或 "http"
（代理本身那条连接、和静态托管那条，牌子上分两段印）。

	绑在一个具体地址上时就是它自己；绑在 0.0.0.0 / :: 上（装服务器上那种装法）
	时把每张网卡的地址各列一条 —— 那时候 ln.Addr() 是 "0.0.0.0:8181" 这种
	谁也连不上的写法，照着它去填面板是要白折腾一轮的。
*/
func localURLs(addr net.Addr, scheme string) []string {
	t, ok := addr.(*net.TCPAddr)
	if !ok {
		return nil
	}
	port := fmt.Sprintf("%d", t.Port)
	if t.IP != nil && !t.IP.IsUnspecified() {
		return []string{scheme + "://" + net.JoinHostPort(t.IP.String(), port)}
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var out []string
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok || ipn.IP.IsLoopback() || ipn.IP.To4() == nil {
			continue /* IPv6 先不列，面板那边填的是 IPv4 */
		}
		out = append(out, scheme+"://"+net.JoinHostPort(ipn.IP.String(), port))
	}
	return out
}

func isLoopback(a net.Addr) bool {
	t, ok := a.(*net.TCPAddr)
	if !ok {
		return false
	}
	return t.IP.IsLoopback()
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
