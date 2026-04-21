import streamlit as st

# 1. 사이트 전체 공통 설정 (반드시 최상단에 위치)
st.set_page_config(
    page_title="Amber Revenue Universe",
    page_icon="🏛️",
    layout="wide"
)

# 2. 페이지(메뉴) 정의
page1 = st.Page("app1_command.py", title="Command Center v8.0", icon="🏨")
page2 = st.Page("app2_market.py", title="Market Intelligence", icon="⚔️")
page3 = st.Page("app3_secret.py", title="Secret Strategy Lab", icon="🚀")

# 3. 네비게이션 사이드바 생성 및 실행
pg = st.navigation({
    "메인 시스템": [page1],
    "시장 및 전략 분석": [page2, page3]
})

pg.run()
