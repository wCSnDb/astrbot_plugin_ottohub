# AstrBot OttoHub Adapter (astrbot_plugin_ottohub)

AstrBot 的 OttoHub 平台适配器插件，支持将 AstrBot 接入到 OttoHub 机器人平台。
这算是我做了比较久的一个项目，目前已经上传到官方的插件仓库，虽然还在审核。
本来是专门为了typer[（点击去typer主页）](https://www.ottohub.cn/u/20032)而制作的一个插件，但是后来因为个人意愿而将其放出。

## 更新日志

### 0.1.6
- 修复收款码工具、meme_manager 独立表情包等"主动发送"消息实际发不出去的问题（此前平台未实现 `send_by_session`，会静默失败）
- 主动发送的消息现在会正确挂在对应的评论线程/私信下，而不是发到帖子根评论区
- 修复紧挨着连续发送容易被 OttoHub 判定为 too_many_requests 的问题：新增可配置的最小发送间隔 + 失败重试
- 修复分段回复只有第一条带 @ 前缀的问题，现在每一条都会带上
- 修复与 meme_manager "独立消息发送"表情包选项的冲突（此前会被强行内联进正文）
- 新增第三方图床兜底：自带图床上传失败后可切换到 Cloudflare R2 / StarDots / 蜜蜂图床（可选是否作为主图床、是否兜底回退）
- 新增单会话模式：私信和所有帖子的评论共享同一个会话/上下文，不再按帖子分别维护，切换帖子时会提示模型"新帖子"；`/reset` 仍可清除该共享上下文

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
