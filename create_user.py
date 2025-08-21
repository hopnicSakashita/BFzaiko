# create_user.py
from flask import Flask
from werkzeug.security import generate_password_hash
import sys
import os
from dotenv import load_dotenv
from app.database import get_db_session
from sqlalchemy import text

# .envファイルを読み込む
load_dotenv()

def create_user(user_id, username, password, custom_db_params=None):
    """データベースに新しいユーザーを作成する"""
    session = None
    try:
        session = get_db_session()
        
        # パスワードをハッシュ化
        hashed_password = generate_password_hash(password)
        
        # ユーザーの存在確認
        existing_user = session.execute(
            text("SELECT USER_ID FROM USER_MST WHERE USER_ID = :user_id"),
            {'user_id': user_id}
        ).first()
        
        if existing_user:
            print(f"警告: ユーザーID '{user_id}' は既に存在します。上書きします。")
            
            # 既存ユーザーの更新
            session.execute(
                text("""
                    UPDATE USER_MST 
                    SET USER_NM = :username,
                        USER_PW = :password,
                        USER_FLG = 0
                    WHERE USER_ID = :user_id
                """),
                {
                    'username': username,
                    'password': hashed_password,
                    'user_id': user_id
                }
            )
        else:
            # 新規ユーザーの作成
            session.execute(
                text("""
                    INSERT INTO USER_MST (USER_ID, USER_NM, USER_PW, USER_FLG)
                    VALUES (:user_id, :username, :password, 0)
                """),
                {
                    'user_id': user_id,
                    'username': username,
                    'password': hashed_password
                }
            )
        
        # 変更をコミット
        session.commit()
        print(f"ユーザー '{user_id}' を正常に登録しました。")
        return True
    except Exception as e:
        if session:
            session.rollback()
        print(f"エラーが発生しました: {str(e)}")
        return False
    finally:
        if session:
            session.close()

if __name__ == "__main__":
    # コマンドライン引数のチェック
    if len(sys.argv) < 4:
        print("使用方法: python create_user.py <ユーザーID> <ユーザー名> <パスワード>")
        print("例: python create_user.py admin 管理者 admin123")
        
        user_id = input("ユーザーID: ")
        username = input("ユーザー名: ")
        password = input("パスワード: ")
    else:
        user_id = sys.argv[1]
        username = sys.argv[2]
        password = sys.argv[3]
    
    # ユーザー作成の実行
    create_user(user_id, username, password)

    # 主要なユーザーを一括登録する例
    create_more = input("他のユーザーも作成しますか？(y/n): ").lower() == 'y'
    if create_more:
        sample_users = [
            ('user1', '一般ユーザー1', 'user123'),
            ('guest', 'ゲストユーザー', 'guest123')
        ]
        
        for user in sample_users:
            create_more = input(f"{user[0]}（{user[1]}）を作成しますか？(y/n): ").lower() == 'y'
            if create_more:
                create_user(user[0], user[1], user[2])

    print("処理が完了しました。")