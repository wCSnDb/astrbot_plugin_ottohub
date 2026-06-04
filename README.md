# AstrBot OttoHub Adapter (astrbot_plugin_ottohub)

AstrBot 的 OttoHub 平台适配器插件，支持将 AstrBot 接入到 OttoHub 机器人平台。
这算是我做了比较久的一个项目，目前因为不知道怎么上传到Astrbot的官方插件仓库，因此就没上传。
本来是专门为了typer[（点击去typer主页）](https://www.ottohub.cn/u/20032)而制作的一个插件，但是后来因为个人意愿而将其放出。

## 安装方法：
一：从链接安装
1. 在AstrBot的WebUI里：
2. 找到侧边栏的插件
3. 然后点下面的AstrBot 插件
4. 接着点击右下角的那个加号
5. 在弹出的菜单内点击链接安装然后填入下面的url，即可安装插件。
   ```text
   https://github.com/wCsNDb/astrbot_plugin_ottohub
   ```

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

别的我也没什么可以说的了，如果有bug可以给我汇报[点击查看到我的主页](https://www.ottohub.cn/u/7510)
