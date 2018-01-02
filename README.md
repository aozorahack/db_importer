# Aozora DB Importer
青空文庫の情報を、Pubserver/Pubserver2で利用できるようにDBに投入するためのツールです。

## 利用方法
```
$ git clone https://github.com/aozorahack/db_importer.git
$ cd db_importer
$ npm install
$ cp .env.sample .env
$ vi .env # 環境変数を適宜修正
$ npm run execute
```

### 用意するもの
- Node.js
- MongoDB or PostgreSQL

### 環境変数

| 名称 | 意味 |
| ----- | ---- |
| `AOZORA_MONGODB_HOST` | 利用するMongoDBのホスト名|
| `AOZORA_MONGODB_PORT` | 利用するMongoDBのポート番号|
| `AOZORA_MONGODB_CREDENTIAL` | 利用するMongoDBのパスワード|
| `PGHOST` | 利用するPostgreSQLのホスト名 |
| `PGPORT` | 利用するPostgreSQLのポート番号 |

### コマンドライン引数

#### csv2db
- `-r/--refresh`: DBの内容を全て入れ替える(デフォルトは前回アップデート時からの差分のみの更新)
- `-b/--backend`: 利用するバックエンドのDB。今のところ`mongo`と`postgresql`が利用可能。

