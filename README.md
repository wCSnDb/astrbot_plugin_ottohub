# AstrBot OttoHub Adapter (astrbot_plugin_ottohub)

AstrBot 的 OttoHub 平台适配器插件，支持将 AstrBot 接入到 OttoHub 机器人平台。

## ✨ 功能特性

- **平台接入**：提供完整的适配器和客户端实现，无缝对接 OttoHub 平台。
- **消息过滤与清洗**：自动剔除冗余的 `[图片]` 占位符、清理外部 OCR 识别产生的垃圾文本。
- **LLM 预处理**：支持在发送给大语言模型（LLM）前重写 Prompt，以及自定义系统提示词（System Prompt）和回复限制提示词。
- **配置项过滤**：仅在选择 OttoHub 平台时展示相关登录凭证（如 `cookie_json` 和 `user_agent`），界面整洁易用。

## 📥 安装方法

### 方式一：从链接安装（推荐）
1. 打开 AstrBot WebUI，导航到 **插件** -> **安装插件**。
2. 选择 **从链接安装**。
3. 输入本仓库的 Git 地址：
   ```text
   https://github.com/wCsNDb/astrbot_plugin_ottohub
   ```
4. 点击确定，等待安装完成并重载插件即可。

### 方式二：手动安装
1. 克隆本仓库到 AstrBot 的插件目录：
   ```bash
   cd AstrBot/data/plugins
   git clone https://github.com/wCsNDb/astrbot_plugin_ottohub.git
   ```
2. 重启 AstrBot 或在 WebUI 中重载插件。

## ⚙️ 配置说明

在 AstrBot WebUI 的设置或添加机器人页面中：
- 填入您的 `cookie_json` 和 `user_agent` 凭证进行登录对接。
- 支持在设置页面中配置提示词相关的行为控制。

## 📄 开源协议

本项目基于 [GPL-3.0](LICENSE) 协议开源。
