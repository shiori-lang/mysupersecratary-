# mysupersecratary プロジェクト

## 概要
スーパーマーケットの売り上げデータを分析してSNS投稿を自動生成するTelegramボット。

## リポジトリ
- GitHub: https://github.com/shiori-lang/mysupersecratary-
- メインファイル: `supermarket_bot.py`
- main ブランチ → Railway に自動デプロイ

## 技術スタック
- Python 3.11
- Claude API (Anthropic) で売り上げ分析・投稿文生成
- SQLite DB: `/app/data/sales_data.db`（Railwayボリューム永続化）
- Railway でホスティング（プロジェクト: joyful-optimism、サービス: worker）

## Railway
- ログイン: shiori@betrnk-tours.com
- プロジェクト: joyful-optimism → production → worker
- main への push で自動デプロイされる

## 環境変数（Railway ダッシュボードで管理）
- `DB_PATH`: /app/data/sales_data.db
- `OWNER_CHAT_ID`: 8369866209
- `SCHEDULE_REPLY_CHAT_ID`: -4845840580
- `WEEKLY_REPORT_CHAT_ID`: -4845840580
- `STORE_GROUP_IDS`: 8グループのID一覧

## Claudeが直接デプロイできるようにする（進行中）
GitHub に push できるよう Personal Access Token の設定が必要。
- GitHubのFine-grained tokenを `mysupersecratary-` リポジトリに設定する
- Permissions: Contents → Read and write
- 設定したらgitの認証情報に保存する

## Playwright MCP（設定済み）
- `~/.claude/mcp.json` に設定済み
- ターミナルの `claude` からのみ使用可能（VSCode拡張では未対応）

## よく使うコマンド
```bash
# ログ確認
railway logs --tail 100

# ローカルでbot起動
cd /private/tmp/mysupersecratary
python supermarket_bot.py

# push（認証設定後）
git -C /private/tmp/mysupersecratary add -p
git -C /private/tmp/mysupersecratary commit -m "..."
git -C /private/tmp/mysupersecratary push
```

## 実装済み機能
- 日次レポート（COMMENTフィールド対応）
- 週次レポート（月曜8am PHT に自動送信）
- 重複削除コマンド（オーナー用）
- DB診断コマンド
- 24時間後メッセージ自動削除
- 変更があった場合のみ再分析（重複スキップ）
