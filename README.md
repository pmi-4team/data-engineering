echo "# data-engineering" > README.md
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/pmi-4team/data-engineering.git
git push -u origin main

<hr>
<h1>postgreSQL 데이터베이스 연결 및 데이터 삽입</h1>

# insert_users.py
- qpoll 파일들과 welcome1 & welcome2 파일 비교 데이터
- 테이블 : users
  
# insert_poll2.py
- qpoll 데이터 삽입 가능
- 테이블 : polls, poll_options, user_poll_responses

# insert_profile2.py
- welcome 데이터 삽입 가능
- 테이블 : profile_questions, user_profile_answers
