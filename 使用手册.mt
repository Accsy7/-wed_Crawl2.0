1对应库的安装，在控制台出现缺少对应库的运行则ctrl点击缺少的库后自动安装。     2调整控制台输出速度，则在output.py中查找：“静默延迟”
3.项目结构：
wed_Crawl/
    ├── Crawling.py              # 主程序入口 - 可安全展示
    ├── output.py               # 输出控制模块 - 可安全展示，延迟控制在这里
    ├── network_session.py      # 网络会话模块 -
    ├── data_processor.py       # 数据处理模块 - 私有配置（不可公开）
    ├── config_secret.py        # 敏感配置模块 - 私有配置（不可公开）
    └──data                     #相应软路径缓存数据
    ├   ├──acctedu
    ├   ├──chinatax
    ├   ├──dnr
    ├   ├──gxzf
    ├   └──tjj
    └──pyivate
         └──__init__.py  #未来开发修改，将非必要展示的文件移动集成为该文件包



4原始文件路径采用软编码的方式，使用data文件夹包含了原始数据。
5若要真正保存.db文件。则需要查询相应的config_secret.py代码中：“ "database_path": ”，对后方的路径进行修改即可。