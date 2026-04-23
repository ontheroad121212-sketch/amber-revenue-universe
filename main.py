import streamlit as st

# 1. 사이트 전체 공통 설정 (반드시 최상단에 위치)
st.set_page_config(
    page_title="Amber Revenue Universe",
    page_icon="🏛️",
    layout="wide"
)

# 2. 통합 로그인 시스템
def check_password():
    # 세션에 로그인 상태가 없으면 False로 초기화
    if "global_authenticated" not in st.session_state:
        st.session_state["global_authenticated"] = False

    # 로그인이 안 되어 있다면 로그인 화면만 표시
    if not st.session_state["global_authenticated"]:
        st.title("🔐 Amber Revenue Universe")
        st.caption("통합 수익관리 시스템 - 관계자 외 출입 금지")
        pw = st.text_input("접속 암호", type="password")
        
        if st.button("접속"):
            if pw == "0822":  # 💡 원하시는 통합 비밀번호로 변경 가능합니다.
                st.session_state["global_authenticated"] = True
                st.rerun()
            else:
                st.error("암호가 틀렸습니다.")
        return False
    return True

# 3. 로그인 성공 시에만 네비게이션 메뉴 실행
if check_password():
    # 페이지(메뉴) 정의
    page1 = st.Page("app1_command.py", title="Command Center v8.0", icon="🏨")
    page2 = st.Page("app2_market.py", title="Market Intelligence", icon="⚔️")
    page3 = st.Page("app3_secret.py", title="Secret Strategy Lab", icon="🚀")

    # 네비게이션 사이드바 생성 및 실행
    pg = st.navigation({
        "메인 시스템": [page1],
        "시장 및 전략 분석": [page2, page3]
    })

    pg.run()
