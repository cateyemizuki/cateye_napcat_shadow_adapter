"""NapCat 影子适配器插件包（MaiBot v2 插件）。

入口为 plugin.py 中的 create_plugin()。同目录模块（relay_core / onebot_client）
经相对导入（from . import ...）引用——MaiBot Runner 加载 plugin.py 时只把插件
根目录（plugins/）放入 sys.path，插件自身目录通过包 __path__ 定位。
"""
