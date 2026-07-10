# AstrBot OttoHub Adapter (astrbot_plugin_ottohub)

AstrBot 的 OttoHub 平台适配器插件，支持将 AstrBot 接入到 OttoHub 机器人平台。
这算是我做了比较久的一个项目，目前已经上传到官方的插件仓库，虽然还在审核。
本来是专门为了typer[（点击去typer主页）](https://www.ottohub.cn/u/20032)而制作的一个插件，但是后来因为个人意愿而将其放出。

## 更新日志

### 0.1.6
- 实现 `send_by_session`，支持更多的插件
- 新增可配置的最小发送间隔 + 失败重试
- 新增第三方图床支持
- 新增单会话模式：私信和所有帖子的评论共享同一个会话/上下文，不再按帖子分别维护
- 修复一些bug，如@前缀在多个分段消息下可能会部分不附带等

## 安装方法：
一：Astrbot 插件市场安装：
1. 打开 Astrbot 插件市场页面
2. 点击安装

二：手动安装
1. 克隆本仓库到 AstrBot 的插件目录：
   ```bash
   cd AstrBot/data/plugins
   git clone https://github.com/wCsNDb/astrbot_plugin_ottohub.git
   ```
2. 重启AstrBot。

## 配置：

在AstrBot WebUI的添加机器人页面中：
- 填写cookie_json（你的ottohub的cookies（json格式））和user_agent（这个暂时不用管）凭证进行登录对接。
- 你可以在设置页面中配置相关的设置，比如说图片上传和信息附带、回复处等。

别的我也没什么可以说的了，如果有bug可以创建议题
