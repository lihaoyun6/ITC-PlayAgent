# ITC 播放代理
适用于 ITC T-7700 系列的 UDP PCM 裸流转发代理工具, 配套 ITC-BellControl 项目使用可在浏览器中实现实时对讲和在线播放功能  

## 使用方法
1. 前往 [发布页面](https://github.com/lihaoyun6/ITC-PlayAgent/releases/latest) 下载  
2. 将其中的 `play-agent.exe` 可执行文件解压到任意位置
3. 参考下列命令行参数进行使用

```bash
> play-agent.exe -h
用法：play-agent [选项]

选项：
  --listen string
    	监听地址（默认 0.0.0.0:8181，局域网里别的机器也能连；只想本机连填 127.0.0.1:8181） (default "0.0.0.0:8181")
  --origin string
    	额外允许的 Origin，逗号分隔（面板在别的主机名上时才要填）
  --web-dir string
    	网页目录。给了它就顺带开静态托管，不给就不开
  --web-listen string
    	静态托管的监听地址（默认 0.0.0.0:8182，只在给了 --web-dir 时才有意义） (default "0.0.0.0:8182")
```
