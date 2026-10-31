# 抖音续火花（本机单用户版）

这是一个在个人电脑上运行的抖音好友互动自动化工具。**每位使用者都应单独克隆一份，在自己的电脑上登录自己拥有或获明确授权的账号，并维护自己的好友、文案和运行数据。**它不是多人共用的在线服务，也不是抖音官方工具。

## 重要提示

- 本应用会自动化操作抖音网页并发送私信。抖音当前的[用户服务协议](https://www.douyin.com/draft/douyin_agreement/douyin_agreement_user.html?id=6773906068725565448)和[社区自律公约](https://sf3-cdn-tos.douyinstatic.com/obj/ies-hotsoon-draft/douyin_agreement/b94d5a1e-c676-4e99-92b5-2d5e26d2568b.html?enter_from=user_agreement)对自动化工具和自动化私信有相关限制。使用前请自行查看适用于你的最新规则；平台可能要求验证、限制功能或封禁账号。本项目不保证平台许可或消息送达。
- 面板只绑定 `127.0.0.1`，没有面向多人使用的应用登录。不要把服务暴露到局域网或互联网，也不要与他人共用同一份 `data/`。
- Cookie、好友名单、文案、发送记录和浏览器登录环境都属于个人数据。不要把它们发给别人、提交到 Git 或放进公开 issue。
- 发送引擎包含来自上游项目的代码，许可与来源见 [`NOTICE.md`](NOTICE.md) 和 [`LICENSE`](LICENSE)。项目按非商业用途分享；不要将其重新标为 MIT、Apache 等更宽松许可证。

## 环境

- macOS 优先；系统通知和可选的开机启动使用 macOS 工具。
- Python 3.11 或更高版本。
- 首次安装需要下载 Playwright Chromium 浏览器。

## 安装与首次运行

在终端中执行：

```bash
git clone https://github.com/xiumao0131-cloud/douyin-spark-local.git douyin-spark
cd douyin-spark
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m playwright install chromium
cp data/config.example.json data/config.json
cp data/messages.example.json data/messages.json
.venv/bin/python -m spark serve
```

打开 <http://127.0.0.1:8899>，用自己的账号扫码登录。面板初始配置为暂停状态；确认账号、好友名单和文案都正确后，再手动恢复发送。示例文案池是空的，需要在面板中添加自己准备的文案。

运行数据保存在当前克隆目录的 `data/`、`logs/` 和 `reports/` 中；这些路径已加入 `.gitignore`。要备份时，请在本机安全保存，不要把真实数据复制到分享仓库。

## 常用命令

```bash
.venv/bin/python -m spark status --json
.venv/bin/python -m spark pause
.venv/bin/python -m spark resume
.venv/bin/python -m spark run
```

## 可选：macOS 开机启动

`launchd/com.example.spark.plist.template` 是模板，不能原样安装。先在项目目录运行 `pwd`，然后复制模板到 `~/Library/LaunchAgents/com.example.spark.plist`，把其中每个 `/ABSOLUTE/PATH/TO/PROJECT` 替换成项目的绝对路径，并创建日志目录：

```bash
mkdir -p logs ~/Library/LaunchAgents
```

配置完成后加载：

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.example.spark.plist
```

如果你安装多份副本，每份都要使用不同的 LaunchAgent `Label` 和端口。

## 开发

```bash
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m pytest tests/ -q
```

## 来源与许可

发送引擎基于 [DouYinSparkFlow](https://github.com/halfwaystudent/douyin-sparkflow) 改编。分享和使用受上游 PolyForm Noncommercial License 1.0.0 条款约束；请阅读随仓库提供的许可证文本。第三方依赖各自遵循其许可证。
