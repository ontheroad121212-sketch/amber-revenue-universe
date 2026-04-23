import streamlit as st
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import os
import requests
import time
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# =============================================================================
# 1. 시스템 설정 및 세션 초기화
# =============================================================================

if 'otb_data' not in st.session_state:
    st.session_state['otb_data'] = pd.DataFrame()

# =========================================================================
# 2. 글로벌 DB 완벽 연결 (중복 연결 에러 원천 차단)
# =========================================================================
# 1) 기본 앱 (viva_key.json)
if not firebase_admin._apps:
    try:
        cred = credentials.Certificate("viva_key.json")
        firebase_admin.initialize_app(cred)
    except: pass

# 2) 호텔 DB 앱
try:
    app_hotel = firebase_admin.get_app("hotel_app")
except ValueError:
    if "firebase" in st.secrets:
        cred_h = credentials.Certificate(dict(st.secrets["firebase_hotel"]))
        app_hotel = firebase_admin.initialize_app(cred_h, name="hotel_app")
    else:
        app_hotel = None
db_hotel = firestore.client(app=app_hotel) if app_hotel else None

# 3) 항공/전략 DB 앱
try:
    app_flight = firebase_admin.get_app("flight_app")
except ValueError:
    if "firebase_flight" in st.secrets:
        secret_dict = dict(st.secrets["firebase_flight"])
        if "private_key" in secret_dict:
            secret_dict["private_key"] = secret_dict["private_key"].replace("\\n", "\n")
        cred_f = credentials.Certificate(secret_dict)
        app_flight = firebase_admin.initialize_app(cred_f, name="flight_app")
    else:
        try:
            cred_f = credentials.Certificate("serviceAccountKey.json")
            app_flight = firebase_admin.initialize_app(cred_f, name="flight_app")
        except:
            app_flight = None
db_flight = firestore.client(app=app_flight) if app_flight else None


# =========================================================================
# 3. 데이터 로딩 함수 정의 
# =========================================================================
@st.cache_data(ttl=600)
def parse_uploaded_files(files):
    if not files: return pd.DataFrame()
    all_data = []
    for file in files:
        try:
            if file.name.endswith('.csv'): df = pd.read_csv(file, header=3) 
            else: df = pd.read_excel(file, header=3)
            
            df.columns = [str(c).strip() for c in df.columns]
            rev_cols = [c for c in df.columns if '매출' in c]
            date_cols = [c for c in df.columns if '일자' in c]
            
            if not rev_cols or not date_cols: continue
            
            target_date = date_cols[0]
            target_rev = rev_cols[-1]
            
            room_cols = [c for c in df.columns if '객실수' in c or '판매객실' in c]
            target_room = room_cols[-1] if room_cols else None
            
            cols_to_use = [target_date, target_rev]
            if target_room: cols_to_use.append(target_room)
            
            temp = df[cols_to_use].copy()
            new_cols = ['date', 'otb_revenue']
            if target_room: new_cols.append('rooms_sold')
            temp.columns = new_cols
            
            temp = temp[~temp['date'].astype(str).str.replace(' ','').str.contains('소계|합계|총합계|Total|Subtotal', case=False, na=False)]
            temp['date'] = pd.to_datetime(temp['date'], errors='coerce')
            temp = temp.dropna(subset=['date']) 
            
            if not temp.empty:
                temp['date'] = temp['date'].dt.tz_localize(None).dt.normalize()
                for col in ['otb_revenue', 'rooms_sold']:
                    if col in temp.columns:
                        if temp[col].dtype == object:
                            temp[col] = temp[col].astype(str).str.replace(',', '').str.replace(' ', '')
                        temp[col] = pd.to_numeric(temp[col], errors='coerce').fillna(0)
                if 'rooms_sold' not in temp.columns: temp['rooms_sold'] = 0
                all_data.append(temp)
        except: continue
    
    if all_data:
        final_df = pd.concat(all_data).sort_values('date').reset_index(drop=True)
        return final_df.drop_duplicates(subset=['date'], keep='last')
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_flight_data_only():
    try:
        if not db_flight: return pd.DataFrame()
        docs = db_flight.collection('flight_prices').stream()
        data = [d.to_dict() for d in docs]
        df = pd.DataFrame(data)
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.tz_localize(None).dt.normalize()
            if 'search_date_str' in df.columns: df['search_date_str'] = df['search_date_str'].fillna('').astype(str)
            else: df['search_date_str'] = 'unknown'
            return df.sort_values('date')
        return pd.DataFrame()
    except: return pd.DataFrame()

@st.cache_data(ttl=600)
def get_comp_data_only():
    try:
        if not db_flight: return pd.DataFrame()
        docs = db_flight.collection('hotel_comp_prices').stream()
        data = [d.to_dict() for d in docs]
        df = pd.DataFrame(data)
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.tz_localize(None).dt.normalize()
            if 'search_date_str' in df.columns: df['search_date_str'] = df['search_date_str'].fillna('').astype(str)
            return df.sort_values('date')
        return pd.DataFrame()
    except: return pd.DataFrame()

# ============================================================
# [원 대시보드.py 전용] 경쟁사 가격 데이터 로더
# 파일1의 load_data() 함수를 이름 변경해서 그대로 이식
# (파일1 탭들에서 사용하는 컬럼 구조가 달라서 별도 유지)
# ============================================================
@st.cache_data(ttl=600)
def load_hotel_comp_data_legacy():
    """원래 대시보드.py의 load_data() 함수. 파일1 탭 3개에서 사용."""
    if not db_flight:
        return pd.DataFrame()
    try:
        docs = db_flight.collection('hotel_comp_prices').stream()
        
        data = []
        for doc in docs:
            d = doc.to_dict()
            row = {
                "CheckIn_Date": d.get("date"),            # 숙박 예정일 (예: 2026-03-28)
                "Search_Date": d.get("search_date_str"),  # 크롤링한 날짜 (예: 20260206)
                "Hotel": d.get("hotel_name"),
                "Min_Price": d.get("price"),
                "Amber_Twin": d.get("hill_amber_twin"),    # 엠버 전용
                "Pine_Double": d.get("hill_pine_double"),  # 엠버 전용
                "Timestamp": d.get("crawled_at")
            }
            data.append(row)
        
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        
        # 날짜 형식 변환
        df["CheckIn_Date"] = pd.to_datetime(df["CheckIn_Date"])
        df["Search_Date_DT"] = pd.to_datetime(df["Search_Date"], format="%Y%m%d", errors='coerce')
        
        # 정렬
        df = df.sort_values(by=["CheckIn_Date", "Search_Date_DT"])
        return df
    except:
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_rent_data_only():
    try:
        if not db_flight: return pd.DataFrame()
        docs = db_flight.collection('rental_prices').stream()
        data = [d.to_dict() for d in docs]
        df = pd.DataFrame(data)
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.tz_localize(None).dt.normalize()
            if 'search_date_str' in df.columns: df['search_date_str'] = df['search_date_str'].fillna('').astype(str)
            return df.sort_values('date')
        return pd.DataFrame()
    except: return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_tourist_data_only():
    try:
        if not db_flight: return pd.DataFrame()
        docs = db_flight.collection('tourist_arrivals').stream()
        data = [d.to_dict() for d in docs]
        df = pd.DataFrame(data)
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.tz_localize(None).dt.normalize()
            return df.sort_values('date')
        return pd.DataFrame()
    except: return pd.DataFrame()

@st.cache_data(ttl=600)
def get_db_bookings_only():
    try:
        if not db_hotel: return pd.DataFrame()
        docs = db_hotel.collection('hotel_bookings').stream()
        data = [d.to_dict() for d in docs]
        df = pd.DataFrame(data)
        if not df.empty:
            if '입실일자' in df.columns: df['입실일자'] = pd.to_datetime(df['입실일자'], errors='coerce').dt.tz_localize(None).dt.normalize()
            if '접수일자' in df.columns: df['접수일자'] = pd.to_datetime(df['접수일자'], errors='coerce').dt.tz_localize(None).dt.normalize()
            if '총금액' in df.columns: df['총금액_숫자'] = pd.to_numeric(df['총금액'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        return df
    except: return pd.DataFrame()

# =========================================================================
# 4. 메인 UI 렌더링 시작 (로그인 통과 후 실행)
# =========================================================================

# 1️⃣ 여기는 '본문' 영역입니다 (화면 중앙)
st.title("🚀 2026 비상: 시크릿 전략 페이지")
st.markdown("---")

# 2️⃣ 여기는 '사이드바' 영역입니다 (왼쪽 메뉴)
with st.sidebar:
    st.header("📂 데이터 사령부")

    # [1] 데이터 저장소 초기화
    if 'otb_data' not in st.session_state:
        st.session_state['otb_data'] = pd.DataFrame()

    # [2] 🔥 클라우드 자동 동기화 로직 (APP3에서 올린 최신 데이터 연동)
    if st.session_state['otb_data'].empty and db_flight:
        try:
            latest_docs = db_flight.collection("amber_snapshots").order_by("timestamp", direction=firestore.Query.DESCENDING).limit(1).get()
            if latest_docs:
                d = latest_docs[0].to_dict()
                pms_raw = d.get('pms_data')
                if pms_raw:
                    import io
                    st.session_state['otb_data'] = pd.read_json(io.StringIO(pms_raw), orient='split')
                    st.success(f"☁️ 클라우드 최신 데이터 동기화 완료\n({d.get('save_name')})")
        except Exception as e:
            st.warning(f"자동 동기화 중 오류(무시 가능): {e}")

    # [3] 화면 표시 및 수동 업로드 기능
    if not st.session_state['otb_data'].empty:
        st.success("🔗 APP3(전략 사령부)의 데이터를 자동으로 연동했습니다!")
        st.info(f"✅ 총 {len(st.session_state['otb_data']):,}일치 데이터 로드 됨")
        
        with st.expander("🔄 데이터 수동 덮어쓰기 (파일 업로드)"):
            uploaded_files = st.file_uploader("월별 온북 파일 업로드", type=['csv', 'xlsx'], accept_multiple_files=True, key="manual_up")
            if uploaded_files:
                parsed_df = parse_uploaded_files(uploaded_files)
                if not parsed_df.empty:
                    st.session_state['otb_data'] = parsed_df
                    st.success("✅ 파일 데이터로 교체 완료!")
                    st.rerun()
    else:
        uploaded_files = st.file_uploader("월별 온북 파일 업로드", type=['csv', 'xlsx'], accept_multiple_files=True, key="first_up")
        if uploaded_files:
            parsed_df = parse_uploaded_files(uploaded_files)
            if not parsed_df.empty:
                st.session_state['otb_data'] = parsed_df
                st.success(f"✅ {len(parsed_df)}일치 데이터 로드 완료")
                st.rerun()

    st.markdown("---")
    if st.button("🔄 시스템 전체 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # -----------------------------------------------------------------
    # 글로벌 데이터 타임머신 (모든 탭 동기화)
    # -----------------------------------------------------------------
    st.markdown("---")
    st.markdown("### 🕰️ 데이터 타임머신")
    st.caption("선택한 일자에 수집된 시장 데이터를 전체 탭에 일괄 적용합니다.")
    
    df_f_side = get_flight_data_only()
    df_c_side = get_comp_data_only()
    df_r_side = get_rent_data_only()

    all_dates = set()
    if not df_f_side.empty: all_dates.update(df_f_side['search_date_str'].dropna().unique())
    if not df_c_side.empty: all_dates.update(df_c_side['search_date_str'].dropna().unique())
    if not df_r_side.empty: all_dates.update(df_r_side['search_date_str'].dropna().unique())
    
    all_search_dates = sorted([d for d in all_dates if d not in ['unknown', '']], reverse=True)

    if all_search_dates:
        selected_global_date = st.selectbox("조회할 크롤링 수집일 선택", all_search_dates, index=0)
        st.session_state['global_search_date'] = selected_global_date
    else:
        st.session_state['global_search_date'] = "-"
        st.info("수집된 크롤링 데이터가 없습니다.")

    st.markdown("---")

    # 날씨 API
    api_key = st.secrets.get("data_portal_key", None)
    col_w1, col_w2 = st.columns([1, 2])
    with col_w1: st.subheader("🌤️ 제주 날씨")
    with col_w2:
        if api_key:
            try:
                url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
                now = datetime.now()
                base_date = now.strftime("%Y%m%d")
                base_time = "2300" if now.hour == 0 else f"{now.hour:02d}00"
                if now.minute < 40:
                    if now.hour == 0: base_date = (now - pd.Timedelta(days=1)).strftime("%Y%m%d")
                    else: base_time = f"{now.hour-1:02d}00"
                res = requests.get(url, params={"serviceKey": api_key, "pageNo": "1", "numOfRows": "100", "dataType": "JSON", "base_date": base_date, "base_time": base_time, "nx": "52", "ny": "38"}, timeout=3)
                if res.status_code == 200:
                    items = res.json()['response']['body']['items']['item']
                    w_data = {i['category']: i['obsrValue'] for i in items if i['category'] in ['T1H', 'RN1', 'REH']}
                    m1, m2, m3 = st.columns(3)
                    m1.metric("기온", f"{w_data.get('T1H', '-')}°C")
                    m2.metric("강수량", f"{w_data.get('RN1', '0')}mm")
                    m3.metric("습도", f"{w_data.get('REH', '-')}%")
            except: st.caption("날씨 정보 수신 대기중...")
        else: st.caption("API Key 설정 필요")

    st.markdown("---")

    # 🕰️ 전략 스냅샷 복원 (전략 사령부 탭 전용)
    st.markdown("### 🏛️ 전략 스냅샷 복원")
    snapshot_options = ["선택 안 함"]
    snap_map = {}
    
    if db_flight:
        try:
            docs = db_flight.collection('amber_snapshots').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(20).stream()
            snap_map = {doc.id: doc.to_dict().get('save_name', doc.id) for doc in docs}
            snapshot_options.extend(list(snap_map.keys()))
        except: pass
    
    selected_snap_id = st.selectbox("불러올 스냅샷 시점", snapshot_options, format_func=lambda x: snap_map.get(x, x))
    
    if selected_snap_id != "선택 안 함":
        if st.button("📥 해당 시점으로 OTB 복원", use_container_width=True):
            try:
                target_doc = db_flight.collection('amber_snapshots').document(selected_snap_id).get()
                if target_doc.exists:
                    d = target_doc.to_dict()
                    pms_raw = d.get('pms_data')
                    if pms_raw:
                        import io
                        st.session_state['otb_data'] = pd.read_json(io.StringIO(pms_raw), orient='split')
                        st.success(f"✅ {snap_map[selected_snap_id]} 복원 완료!")
                        st.rerun()
            except Exception as e:
                st.error(f"복원 실패: {e}")

# =========================================================================
# 5. 탭 구성 (UI) - 파일1 탭 3개 + 파일2 탭 7개 = 총 10개 탭
# =========================================================================
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs([
    "✈️ 항공권 (수요)",
    "🏨 엠버 예약(분석)",
    "⚔️ 경쟁사 비교",
    "🚗 렌터카 분석",
    "📊 입도객 추이",
    "📜 과거 분석 (Pace)",
    "🧠 전략 사령부 (BI)",
    "🏨 시장 최저가 대시보드",
    "💎 엠버 객실 전략",
    "📈 가격 변동 추이 (Time Travel)"
])

# --- TAB 1: 항공권 ---
with tab1:
    st.header("✈️ 김포 -> 제주 항공권 (수요 강도 & 가격 추적)")
    with st.spinner("항공권 데이터를 불러오는 중입니다..."):
        df_f = get_flight_data_only()
        
    if not df_f.empty:
        dates = sorted(df_f['search_date_str'].unique(), reverse=True)
        global_date = st.session_state.get('global_search_date', '-')
        
        latest = global_date if global_date in dates else (dates[0] if dates else "-")
        st.info(f"💡 현재 화면은 **{latest}**에 수집된 항공권 시장 상황을 보여줍니다.")
        
        if dates:
            df_lat = df_f[df_f['search_date_str'] == latest].copy()
        
            if not df_lat.empty:
                st.subheader("🔥 요일별 요금 히트맵")
                df_lat['day_name'] = df_lat['date'].dt.day_name()
                days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                fig_h = px.density_heatmap(df_lat, x='date', y='day_name', z='min_price', histfunc='avg', 
                                             color_continuous_scale='RdYlGn_r', category_orders={'day_name': days_order})
                st.plotly_chart(fig_h, use_container_width=True)

                st.subheader("📉 전주 대비 가격 변동 (WoW)")
                target_past = (datetime.strptime(latest, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
                past_cands = [d for d in dates if d <= target_past]
                if past_cands:
                    df_p = df_f[df_f['search_date_str'] == past_cands[0]].copy()
                    df_wow = pd.merge(df_lat[['date', 'min_price']], df_p[['date', 'min_price']], on='date', suffixes=('_curr', '_prev'))
                    df_wow['diff'] = df_wow['min_price_curr'] - df_wow['min_price_prev']
                    fig_w = px.bar(df_wow, x='date', y='diff', color='diff', color_continuous_scale='RdBu_r', title=f"가격 변동 ({past_cands[0]} 대비)")
                    st.plotly_chart(fig_w, use_container_width=True)
                else: st.caption("비교할 과거 데이터가 부족합니다.")
                
        st.markdown("---")
        st.subheader("📊 전체 기간 가격 흐름 (전체 수집일)")
        st.line_chart(df_f.pivot_table(index='date', columns='search_date_str', values='min_price'))

        st.markdown("---")
        st.subheader("📉 항공권 가격 정책 변화 감지 (Policy Shift)")
        st.caption("과거 시점 대비 현재의 **항공권 요금 정책(미래 180일)**이 어떻게 바뀌었는지 비교합니다.")

        if len(dates) < 2:
            st.warning("⚠️ 비교할 과거 데이터가 충분하지 않습니다.")
        else:
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                default_curr_idx = dates.index(latest) if latest in dates else 0
                f_date_curr = st.selectbox("기준 조회일 (실선)", dates, index=default_curr_idx, key='f_curr')
            with col_f2:
                def_idx = 1 if len(dates) > 1 else 0
                f_date_prev = st.selectbox("과거 조회일 (점선)", dates, index=def_idx, key='f_prev')

            mask_dates = df_f['search_date_str'].isin([f_date_curr, f_date_prev])
            df_shift_f = df_f[mask_dates].copy()

            if not df_shift_f.empty:
                df_shift_f['조회일'] = pd.to_datetime(df_shift_f['search_date_str'], format='%Y%m%d').dt.strftime('%m-%d')
                df_shift_f = df_shift_f.sort_values(['search_date_str', 'date'])

                fig_shift_f = px.line(df_shift_f, x='date', y='min_price', color='조회일', line_dash='조회일', 
                                          markers=True, title=f"항공권 가격 이동 비교 ({f_date_prev} vs {f_date_curr})",
                                          labels={'date': '탑승 날짜', 'min_price': '최저가(원)', '조회일': '데이터 수집일'})
                fig_shift_f.update_traces(line=dict(width=2))
                st.plotly_chart(fig_shift_f, use_container_width=True)
    else: st.warning("데이터가 없습니다.")

# --- TAB 2: 호텔 OTB ---
with tab2:
    st.header("🏨 엠버퓨어힐 OTB 심화 분석 (Deep Dive)")
    st.caption("실시간 예약 DB(Firebase)를 연동하여, 선택한 기간의 예약 속도(Pace)와 퀄리티를 입체적으로 분석합니다.")

    with st.spinner("예약 데이터를 불러와 필드를 매핑 중입니다..."):
        df_h_raw = get_db_bookings_only()

    if not df_h_raw.empty:
        def safe_to_datetime(series):
            return pd.to_datetime(series, errors='coerce', utc=True)

        if '예약일자' in df_h_raw.columns: df_h_raw['booking_date'] = safe_to_datetime(df_h_raw['예약일자'])
        elif '접수일자' in df_h_raw.columns: df_h_raw['booking_date'] = safe_to_datetime(df_h_raw['접수일자'])
        else: df_h_raw['booking_date'] = pd.NaT

        df_h_raw['checkin_date'] = safe_to_datetime(df_h_raw['입실일자']) if '입실일자' in df_h_raw.columns else pd.NaT
        df_h_raw['checkout_date'] = safe_to_datetime(df_h_raw['퇴실일자']) if '퇴실일자' in df_h_raw.columns else pd.NaT
        
        if '총금액' in df_h_raw.columns: df_h_raw['revenue'] = pd.to_numeric(df_h_raw['총금액'], errors='coerce').fillna(0)
        elif '총금액_숫자' in df_h_raw.columns: df_h_raw['revenue'] = pd.to_numeric(df_h_raw['총금액_숫자'], errors='coerce').fillna(0)
        else: df_h_raw['revenue'] = 0

        mask_valid = df_h_raw['booking_date'].notnull() & df_h_raw['checkin_date'].notnull()
        if mask_valid.any(): df_h_raw.loc[mask_valid, 'lead_time'] = (df_h_raw.loc[mask_valid, 'checkin_date'] - df_h_raw.loc[mask_valid, 'booking_date']).dt.days
        df_h_raw['lead_time'] = df_h_raw['lead_time'].fillna(0)
        
        mask_los = df_h_raw['checkin_date'].notnull() & df_h_raw['checkout_date'].notnull()
        if mask_los.any(): df_h_raw.loc[mask_los, 'los'] = (df_h_raw.loc[mask_los, 'checkout_date'] - df_h_raw.loc[mask_los, 'checkin_date']).dt.days
        df_h_raw['los'] = df_h_raw['los'].fillna(1)
        
        df_h_raw['checkin_month'] = df_h_raw['checkin_date'].dt.strftime('%Y-%m')
        df_h_raw['checkin_day'] = df_h_raw['checkin_date'].dt.day_name()
        
        st.markdown("#### 📅 분석 기간 설정")
        min_dt, max_dt = df_h_raw['checkin_date'].min(), df_h_raw['checkin_date'].max()
        
        if pd.isnull(min_dt) or pd.isnull(max_dt):
            d_start, d_end = datetime.now().date(), datetime.now().date()
        else:
            d_start = datetime.now().date()
            d_end = d_start + timedelta(days=30)
            if d_start < min_dt.date(): d_start = min_dt.date()
            if d_end > max_dt.date(): d_end = max_dt.date()
            
        col_d1, col_d2 = st.columns([2, 1])
        with col_d1:
            sel_dates = st.date_input("체크인 기간 선택", value=(d_start, d_end), min_value=min_dt.date() if pd.notnull(min_dt) else None, max_value=max_dt.date() if pd.notnull(max_dt) else None)
        
        if isinstance(sel_dates, tuple):
            if len(sel_dates) == 2: s_date, e_date = sel_dates
            elif len(sel_dates) == 1: s_date = e_date = sel_dates[0]
            else: s_date, e_date = d_start, d_end
        else: s_date = e_date = sel_dates
        
        mask_period = (df_h_raw['checkin_date'].dt.date >= s_date) & (df_h_raw['checkin_date'].dt.date <= e_date)
        df_h = df_h_raw[mask_period].copy()
        
        st.info(f"선택하신 기간 (**{s_date} ~ {e_date}**)의 예약 데이터 **{len(df_h):,}건**을 분석합니다.")
        st.markdown("---")

        if not df_h.empty:
            total_rev = df_h['revenue'].sum()
            total_bk = len(df_h)
            adr = total_rev / total_bk if total_bk > 0 else 0
            avg_lead = df_h['lead_time'].mean()
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("기간 총 매출", f"{int(total_rev):,}원")
            k2.metric("기간 예약 건수", f"{total_bk:,}건")
            k3.metric("평균 객단가 (ADR)", f"{int(adr):,}원")
            k4.metric("평균 리드타임", f"{avg_lead:.1f}일")
            
            st.markdown("---")
            st.subheader("📈 Booking Pace (선택 기간 기준)")
            target_months = df_h['checkin_month'].unique()
            df_pace = df_h[df_h['checkin_month'].isin(target_months)].copy()
            
            if not df_pace.empty:
                df_pace['booking_order'] = df_pace['lead_time'] * -1 
                pace_chart_data = df_pace.groupby(['checkin_month', 'booking_order'])['revenue'].sum().reset_index()
                pace_chart_data = pace_chart_data.sort_values(['checkin_month', 'booking_order'])
                pace_chart_data['cum_revenue'] = pace_chart_data.groupby('checkin_month')['revenue'].cumsum()
                
                fig_pace = px.line(pace_chart_data, x='booking_order', y='cum_revenue', color='checkin_month',
                                title="월별 누적 매출 달성 곡선 (J-Curve)", line_shape='spline')
                fig_pace.update_xaxes(range=[-180, 0]) 
                st.plotly_chart(fig_pace, use_container_width=True)

            col_detail1, col_detail2 = st.columns(2)
            with col_detail1:
                st.subheader("💰 리드타임별 객단가(ADR)")
                bins = [-1, 0, 3, 7, 14, 30, 60, 90, 180, 365]
                labels = ['DayOf', '0-3일', '4-7일', '8-14일', '15-30일', '31-60일', '61-90일', '90-180일', '180일+']
                df_h['lead_grp'] = pd.cut(df_h['lead_time'], bins=bins, labels=labels)
                grp_stats = df_h.groupby('lead_grp', observed=False).agg(adr=('revenue', 'mean'), count=('revenue', 'count')).reset_index()
                
                fig_lead = make_subplots(specs=[[{"secondary_y": True}]])
                fig_lead.add_trace(go.Bar(x=grp_stats['lead_grp'], y=grp_stats['count'], name="예약 건수", marker_color='#AEC7E8'), secondary_y=False)
                fig_lead.add_trace(go.Scatter(x=grp_stats['lead_grp'], y=grp_stats['adr'], name="평균 ADR", line=dict(color='#FF7F0E', width=3)), secondary_y=True)
                st.plotly_chart(fig_lead, use_container_width=True)

            with col_detail2:
                st.subheader("🍕 채널(거래처) 성과")
                if '거래처' in df_h.columns:
                    top_ch = df_h['거래처'].value_counts().nlargest(10).index
                    df_h['channel_sim'] = df_h['거래처'].apply(lambda x: x if x in top_ch else '기타')
                    ch_stats = df_h.groupby('channel_sim').agg(rev=('revenue', 'sum'), adr=('revenue', 'mean')).reset_index()
                    fig_ch = px.treemap(ch_stats, path=['channel_sim'], values='rev', color='adr', color_continuous_scale='RdBu')
                    st.plotly_chart(fig_ch, use_container_width=True)

            st.markdown("---")
            st.subheader("📅 요일별/월별 예약 집중도")
            if not df_h.empty:
                heat_data = df_h.groupby(['checkin_month', 'checkin_day'])['revenue'].count().reset_index()
                days_ord = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                fig_heat = px.density_heatmap(heat_data, x='checkin_month', y='checkin_day', z='revenue',
                                                category_orders={'checkin_day': days_ord}, color_continuous_scale='Greens')
                st.plotly_chart(fig_heat, use_container_width=True)
    else: st.warning("분석할 예약 데이터가 없습니다.")

# --- TAB 3: 경쟁사 비교 ---
with tab3:
    st.header("⚔️ 경쟁사 가격 포지셔닝 & 추적")
    with st.spinner("경쟁사 데이터를 불러오는 중입니다..."):
        df_c = get_comp_data_only()
        
    if not df_c.empty:
        dates = sorted(df_c['search_date_str'].unique(), reverse=True)
        global_date = st.session_state.get('global_search_date', '-')
        
        latest = global_date if global_date in dates else (dates[0] if dates else "-")
        st.info(f"💡 현재 화면은 **{latest}**에 수집된 경쟁사 요금 기준입니다.")
        
        df_now = df_c[df_c['search_date_str'] == latest].copy()
        
        if not df_now.empty:
            st.subheader("🗺️ 시장 가격 포지셔닝")
            df_others = df_now[df_now['hotel_name'] != 'Amber_Pure_Hill'].groupby('date')['price'].agg(['min','max','mean']).reset_index()
            df_amber = df_now[df_now['hotel_name'] == 'Amber_Pure_Hill'][['date','price']].rename(columns={'price':'amber'})
            df_pos = pd.merge(df_others, df_amber, on='date')
            
            fig_p = go.Figure()
            fig_p.add_trace(go.Scatter(x=df_pos['date'], y=df_pos['max'], mode='lines', line=dict(width=0), showlegend=False))
            fig_p.add_trace(go.Scatter(x=df_pos['date'], y=df_pos['min'], mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(200,200,200,0.5)', name='경쟁사 범위'))
            fig_p.add_trace(go.Scatter(x=df_pos['date'], y=df_pos['mean'], line=dict(color='gray', dash='dot'), name='평균'))
            fig_p.add_trace(go.Scatter(x=df_pos['date'], y=df_pos['amber'], line=dict(color='red', width=3), name='엠버'))
            st.plotly_chart(fig_p, use_container_width=True)
            
            st.subheader("⚡ 경쟁사별 가격 격차 (Price Gap)")
            df_parnas = df_now[df_now['hotel_name'].str.contains('Parnas')][['date','price']].rename(columns={'price':'parnas'})
            df_josun = df_now[df_now['hotel_name'].str.contains('Josun')][['date','price']].rename(columns={'price':'josun'})
            
            df_gap = pd.merge(df_amber, df_parnas, on='date', how='outer')
            df_gap = pd.merge(df_gap, df_josun, on='date', how='outer')
            
            df_gap['파르나스_Gap'] = df_gap['parnas'] - df_gap['amber']
            df_gap['그랜드조선_Gap'] = df_gap['josun'] - df_gap['amber']
            
            df_gap_melt = df_gap.melt(id_vars=['date'], value_vars=['파르나스_Gap', '그랜드조선_Gap'], var_name='competitor', value_name='gap_amount')
            fig_gap = px.bar(df_gap_melt, x='date', y='gap_amount', color='competitor', barmode='group',
                            color_discrete_map={'파르나스_Gap': '#1f77b4', '그랜드조선_Gap': '#2ca02c'})
            st.plotly_chart(fig_gap, use_container_width=True)
            
            st.subheader("🎯 객실별 핀셋 비교")
            df_detail = df_now[df_now['hotel_name']=='Amber_Pure_Hill']
            if 'hill_amber_twin' in df_detail.columns:
                df_twin = df_detail[['date','hill_amber_twin']].copy()
                df_twin['hill_amber_twin'] = pd.to_numeric(df_twin['hill_amber_twin'], errors='coerce')
                
                df_par_min = df_now[df_now['hotel_name'].str.contains('Parnas')][['date','price']].rename(columns={'price':'파르나스_최저'})
                df_jos_min = df_now[df_now['hotel_name'].str.contains('Josun')][['date','price']].rename(columns={'price':'조선_최저'})
                
                df_m = pd.merge(df_twin, df_par_min, on='date', how='outer')
                df_m = pd.merge(df_m, df_jos_min, on='date', how='outer')
                st.plotly_chart(px.line(df_m, x='date', y=['hill_amber_twin','파르나스_최저','조선_최저'], markers=True), use_container_width=True)

        st.markdown("---")
        st.subheader("📉 경쟁사 가격 정책 변화 감지 (Policy Shift)")
        if len(dates) < 2:
            st.warning("⚠️ 비교할 과거 데이터가 충분하지 않습니다.")
        else:
            col_h1, col_h2, col_h3 = st.columns(3)
            with col_h1:
                default_curr_idx = dates.index(latest) if latest in dates else 0
                date_curr = st.selectbox("기준 조회일 (실선)", dates, index=default_curr_idx, key='c_curr')
            with col_h2:
                def_idx = 1 if len(dates) > 1 else 0
                date_prev = st.selectbox("과거 조회일 (점선)", dates, index=def_idx, key='c_prev')
            with col_h3:
                avail_hotels = df_c['hotel_name'].unique()
                sel_hotels = st.multiselect("비교할 호텔 선택", avail_hotels, default=avail_hotels)

            mask_dates = df_c['search_date_str'].isin([date_curr, date_prev])
            mask_hotels = df_c['hotel_name'].isin(sel_hotels)
            df_shift = df_c[mask_dates & mask_hotels].copy()
            
            if not df_shift.empty:
                if 'hill_amber_twin' in df_shift.columns:
                    df_shift['hill_amber_twin'] = pd.to_numeric(df_shift['hill_amber_twin'], errors='coerce')
                    df_shift['price'] = np.where(df_shift['hotel_name'] == 'Amber_Pure_Hill', df_shift['hill_amber_twin'], df_shift['price'])
                
                df_shift = df_shift.dropna(subset=['price'])
                df_shift['조회일'] = pd.to_datetime(df_shift['search_date_str'], format='%Y%m%d').dt.strftime('%m-%d')
                df_shift = df_shift.sort_values(['hotel_name', 'date'])
                
                fig_shift = px.line(df_shift, x='date', y='price', color='hotel_name', line_dash='조회일',
                                    color_discrete_map={'Amber_Pure_Hill': 'red', 'Parnas_Jeju': 'green', 'Grand_Josun': 'blue'})
                fig_shift.update_traces(line=dict(width=2.5))
                st.plotly_chart(fig_shift, use_container_width=True)
    else: st.warning("데이터 없음")
        
# --- TAB 4: 렌터카 ---
with tab4:
    st.header("🚗 렌터카 분석 & 최적 가격 추적")
    with st.spinner("렌터카 시세를 조회 중입니다..."):
        df_r = get_rent_data_only()
        
    if not df_r.empty:
        cols_car = ['Ray_Price', 'K5_Price', 'G80_Price']
        for c in cols_car:
            if c in df_r.columns: df_r[c] = pd.to_numeric(df_r[c], errors='coerce').fillna(0)

        dates = sorted(df_r['search_date_str'].unique(), reverse=True)
        global_date = st.session_state.get('global_search_date', '-')
        
        latest = global_date if global_date in dates else (dates[0] if dates else "-")
        st.info(f"💡 현재 화면은 **{latest}**에 수집된 렌터카 시세 기준입니다.")
        
        df_curr = df_r[df_r['search_date_str'] == latest].copy()

        if not df_curr.empty:
            st.subheader(f"📈 차종별 일별 시세")
            df_melt_curr = df_curr.melt(id_vars=['date'], value_vars=cols_car, var_name='차종', value_name='가격')
            fig_r1 = px.line(df_melt_curr, x='date', y='가격', color='차종', markers=True,
                            color_discrete_map={'Ray_Price': '#2ca02c', 'K5_Price': '#1f77b4', 'G80_Price': '#d62728'})
            st.plotly_chart(fig_r1, use_container_width=True)

        st.markdown("---")
        st.subheader("📉 렌터카 가격 정책 변화 감지 (Policy Shift)")
        if len(dates) < 2:
            st.warning("⚠️ 비교할 과거 데이터가 충분하지 않습니다.")
        else:
            col_rc1, col_rc2, col_rc3 = st.columns(3)
            with col_rc1:
                default_curr_idx = dates.index(latest) if latest in dates else 0
                r_date_curr = st.selectbox("기준 조회일 (실선)", dates, index=default_curr_idx, key='r_curr')
            with col_rc2:
                def_idx = 1 if len(dates) > 1 else 0
                r_date_prev = st.selectbox("과거 조회일 (점선)", dates, index=def_idx, key='r_prev')
            with col_rc3:
                sel_cars = st.multiselect("비교할 차종 선택", cols_car, default=['K5_Price'])

            mask_dates = df_r['search_date_str'].isin([r_date_curr, r_date_prev])
            df_shift_r = df_r[mask_dates].copy()
            df_shift_r = df_shift_r.melt(id_vars=['date', 'search_date_str'], value_vars=sel_cars, var_name='차종', value_name='가격')
            df_shift_r = df_shift_r[df_shift_r['차종'].isin(sel_cars)]

            if not df_shift_r.empty:
                df_shift_r['조회일'] = pd.to_datetime(df_shift_r['search_date_str'], format='%Y%m%d').dt.strftime('%m-%d')
                df_shift_r = df_shift_r.sort_values(['차종', 'date'])

                fig_shift_r = px.line(df_shift_r, x='date', y='가격', color='차종', line_dash='조회일', markers=True)
                fig_shift_r.update_traces(line=dict(width=2))
                st.plotly_chart(fig_shift_r, use_container_width=True)

        st.markdown("---")
        st.subheader("🔥 요일별/일자별 가격 히트맵")
        if not df_curr.empty:
            target_car = st.selectbox("분석할 차종을 선택하세요", cols_car, index=1)
            df_heat = df_curr[['date', target_car]].copy()
            df_heat['Day'] = df_heat['date'].dt.day_name()
            
            days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            fig_heat = px.density_heatmap(df_heat, x='date', y='Day', z=target_car, histfunc='avg', 
                                        category_orders={'Day': days_order}, color_continuous_scale='RdYlGn_r')
            st.plotly_chart(fig_heat, use_container_width=True)
            
            with st.expander("📊 요일별 가격 분포 자세히 보기 (Box Plot)"):
                fig_box = px.box(df_heat, x='Day', y=target_car, points="all", category_orders={'Day': days_order}, color='Day')
                st.plotly_chart(fig_box, use_container_width=True)

    else: st.warning("렌터카 데이터가 없습니다.")

# --- TAB 5: 입도객 분석 ---
with tab5:
    st.header("📊 입도객 분석 (트렌드 & 점유율)")
    with st.spinner("입도객 및 예약 데이터를 분석 중입니다..."):
        df_t = get_tourist_data_only()
        df_h_p = get_db_bookings_only()
        
    if not df_t.empty:
        if 'date' in df_t.columns:
            df_t['year'] = df_t['date'].dt.year
            df_t['month'] = df_t['date'].dt.month
            df_t['mmdd'] = df_t['date'].dt.strftime('%m-%d')
            
        latest_row = df_t.iloc[-1]
        st.subheader(f"📅 기준일: {latest_row['date'].strftime('%Y-%m-%d')}")
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("총 입도객", f"{int(latest_row['total']):,}명")
        m2.metric("내국인", f"{int(latest_row['korean']):,}명")
        m3.metric("외국인", f"{int(latest_row['foreign']):,}명")
        m4.metric("외국인 비중", f"{latest_row.get('foreign_ratio', 0):.1f}%")
        
        st.subheader("📈 내국인 vs 외국인 트렌드 변화")
        df_melt = df_t.melt(id_vars=['date'], value_vars=['korean', 'foreign'], var_name='Type', value_name='Count')
        fig_trend = px.area(df_melt, x='date', y='Count', color='Type', title="일별 입도객 구성 추이")
        st.plotly_chart(fig_trend, use_container_width=True)

        st.subheader("🗓️ 연도별 비교 (Seasonality)")
        df_season = df_t.copy()
        if 'year' in df_season.columns and 'mmdd' in df_season.columns:
            df_season['year_str'] = df_season['year'].astype(str)
            
            # 🌟 변경점 1: 'mmdd' 대신 시간의 흐름인 'date' 기준으로 정렬하여 꼬임 방지
            df_season = df_season.sort_values(by='date')

            sub1, sub2 = st.tabs(["총 입도객 비교", "외국인 급증세"])
            
            with sub1: 
                fig1 = px.line(df_season, x='mmdd', y='total', color='year_str')
                # 🌟 변경점 2: categoryorder를 추가하여 가로축이 무조건 01-01부터 순서대로 나오게 쐐기 박기
                fig1.update_xaxes(type='category', categoryorder='category ascending')
                st.plotly_chart(fig1, use_container_width=True)
                
            with sub2: 
                fig2 = px.line(df_season, x='mmdd', y='foreign', color='year_str')
                fig2.update_xaxes(type='category', categoryorder='category ascending')
                st.plotly_chart(fig2, use_container_width=True)
                
        st.markdown("---")
        if not df_h_p.empty:
            df_daily = df_h_p.groupby('입실일자').size().reset_index(name='my_res')
            df_daily.rename(columns={'입실일자':'date'}, inplace=True)
            df_share = pd.merge(df_t, df_daily, on='date', how='inner')
            
            if not df_share.empty:
                st.subheader("📉 시장 수요 vs 예약 상관관계")
                st.plotly_chart(px.scatter(df_share, x='total', y='my_res', hover_data=['date']), use_container_width=True)
                st.subheader("📅 흐름 비교")
                fig_d = make_subplots(specs=[[{"secondary_y": True}]])
                fig_d.add_trace(go.Scatter(x=df_share['date'], y=df_share['total'], name='입도객', line=dict(color='gray')), secondary_y=False)
                fig_d.add_trace(go.Scatter(x=df_share['date'], y=df_share['my_res'], name='내 예약', line=dict(color='red')), secondary_y=True)
                st.plotly_chart(fig_d, use_container_width=True)
    else: st.warning("입도객 데이터가 없습니다.")

# --- TAB 6: Pace Report ---
with tab6:
    st.header("📜 과거 성과 분석 & Pace Report")
    with st.spinner("데이터 분석 중..."):
        df_pace = get_db_bookings_only()
        df_t = get_tourist_data_only()
    
    st.subheader("1. 과거 성과 복기 (Revenue vs Tourist)")
    if not df_pace.empty and not df_t.empty:
        df_rev_daily = df_pace.groupby('입실일자')['총금액_숫자'].sum().reset_index(name='daily_rev')
        df_rev_daily.rename(columns={'입실일자':'date'}, inplace=True)
        df_backtest = pd.merge(df_rev_daily, df_t, on='date', how='inner')
        
        # 🌟 수정 포인트 1: 선이 왔다 갔다 꼬이지 않도록 날짜순으로 확실하게 정렬
        df_backtest = df_backtest.sort_values('date')
        
        if not df_backtest.empty:
            fig_bt = make_subplots(specs=[[{"secondary_y": True}]])
            fig_bt.add_trace(go.Bar(x=df_backtest['date'], y=df_backtest['daily_rev'], name="매출", marker_color='#FF4B4B', opacity=0.6), secondary_y=False)
            fig_bt.add_trace(go.Scatter(x=df_backtest['date'], y=df_backtest['total'], name="총 입도객", line=dict(color='blue')), secondary_y=True)
            fig_bt.add_trace(go.Scatter(x=df_backtest['date'], y=df_backtest['foreign'], name="외국인", line=dict(color='orange', dash='dot')), secondary_y=True)
            st.plotly_chart(fig_bt, use_container_width=True)
        else: st.info("매출과 입도객 날짜가 겹치지 않습니다.")

    st.markdown("---")
    st.subheader("2. Pace Report (전년 대비 성장 속도)")
    if not df_pace.empty and '입실일자' in df_pace.columns:
        df_pace['year'] = df_pace['입실일자'].dt.year
        df_pace['mmdd'] = df_pace['입실일자'].dt.strftime('%m-%d')
        this_y, last_y = datetime.now().year, datetime.now().year - 1
        
        df_ty = df_pace[df_pace['year']==this_y].groupby('mmdd')['총금액_숫자'].sum().reset_index()
        df_ly = df_pace[df_pace['year']==last_y].groupby('mmdd')['총금액_숫자'].sum().reset_index()
        
        df_ty['cumsum'] = df_ty['총금액_숫자'].cumsum()
        df_ly['cumsum'] = df_ly['총금액_숫자'].cumsum()
        
        # 🌟 수정 포인트 2: outer 병합 시 순서가 뒤죽박죽될 수 있으므로, 반드시 'mmdd'로 정렬한 뒤 ffill(빈칸 채우기)을 해야 정상적으로 선이 이어집니다.
        df_comp = pd.merge(df_ty, df_ly, on='mmdd', how='outer', suffixes=(f'_{this_y}', f'_{last_y}'))
        df_comp = df_comp.sort_values('mmdd').ffill().fillna(0)
        
        fig_pc = go.Figure()
        fig_pc.add_trace(go.Scatter(x=df_comp['mmdd'], y=df_comp[f'cumsum_{last_y}'], name=f"{last_y} 누적", line=dict(color='gray', dash='dot')))
        fig_pc.add_trace(go.Scatter(x=df_comp['mmdd'], y=df_comp[f'cumsum_{this_y}'], name=f"{this_y} 누적", line=dict(color='red', width=3)))
        
        # 🌟 수정 포인트 3: 여기서도 가로축을 카테고리로 강제 지정하고 순서를 고정해 줍니다.
        fig_pc.update_xaxes(type='category', categoryorder='category ascending')
        
        st.plotly_chart(fig_pc, use_container_width=True)

# --- TAB 7: 전략 사령부 (BI) ---
with tab7:
    st.header("🎯 180일 수익 최적화 & 인사이트")
    st.caption("모든 데이터를 통합 분석하고, 전략을 저장/비교합니다.")
    
    global_date = st.session_state.get('global_search_date', '-')
    
    with st.spinner("전략 데이터를 통합하는 중입니다..."):
        df_f = get_flight_data_only()
        df_c = get_comp_data_only()
        df_r = get_rent_data_only()
        df_uploaded = st.session_state.get('otb_data', pd.DataFrame())

    if not df_f.empty: 
        latest_f = global_date if global_date in df_f['search_date_str'].values else df_f['search_date_str'].max()
        df_f = df_f[df_f['search_date_str'] == latest_f].groupby('date')['min_price'].min().reset_index().rename(columns={'min_price': '항공권'})
    
    if not df_r.empty:
        latest_r = global_date if global_date in df_r['search_date_str'].values else df_r['search_date_str'].max()
        df_r = df_r[df_r['search_date_str'] == latest_r].groupby('date')['K5_Price'].mean().reset_index().rename(columns={'K5_Price': '렌터카'})
        
    df_p, df_j, df_a = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if not df_c.empty:
        latest_c = global_date if global_date in df_c['search_date_str'].values else df_c['search_date_str'].max()
        df_c_filtered = df_c[df_c['search_date_str'] == latest_c]
        df_p = df_c_filtered[df_c_filtered['hotel_name'].str.contains('Parnas')].groupby('date')['price'].min().reset_index().rename(columns={'price': '파르나스'})
        df_j = df_c_filtered[df_c_filtered['hotel_name'].str.contains('Josun')].groupby('date')['price'].min().reset_index().rename(columns={'price': '그랜드조선'})
        df_a = df_c_filtered[df_c_filtered['hotel_name'].str.contains('Amber')].groupby('date')['hill_amber_twin'].min().reset_index().rename(columns={'hill_amber_twin': '엠버트윈'})

    TOTAL_ROOMS = 128
    if not df_uploaded.empty:
        master = df_uploaded.copy()
        master['date'] = pd.to_datetime(master['date']).dt.tz_localize(None).dt.normalize()
        master['otb_revenue'] = pd.to_numeric(master['otb_revenue'], errors='coerce').fillna(0)
        master['rooms_sold'] = pd.to_numeric(master['rooms_sold'], errors='coerce').fillna(0)
        master['occ'] = (master['rooms_sold'] / TOTAL_ROOMS) * 100
        master['adr'] = master.apply(lambda x: x['otb_revenue'] / x['rooms_sold'] if x['rooms_sold'] > 0 else 0, axis=1)
        
        min_d, max_d = master['date'].min().strftime('%Y-%m-%d'), master['date'].max().strftime('%Y-%m-%d')
        st.success(f"✅ 데이터 로드 성공: **{min_d}** 부터 **{max_d}** 까지 (총 {len(master):,}일)")
    else:
        master = pd.DataFrame({'date': pd.date_range(start=datetime.now().date(), periods=180)})
        master['date'] = pd.to_datetime(master['date']).dt.normalize()

    for part in [df_f, df_p, df_j, df_a, df_r]:
        if not part.empty: master = pd.merge(master, part, on='date', how='outer')
    
    df_view = master.sort_values('date').fillna(0)

    # 전략 저장 버튼
    st.markdown("---")
    c_save1, c_save2 = st.columns([4, 1])
    with c_save2:
        if st.button("💾 오늘의 전략 저장 (DB)", type="primary", use_container_width=True):
            try:
                save_df = df_view.copy()
                save_df['date'] = save_df['date'].dt.strftime('%Y-%m-%d')
            
                doc_id = datetime.now().strftime("%Y-%m-%d_%H%M")
                db_flight.collection('strategy_history').document(doc_id).set({
                    'created_at': datetime.now(),
                    'data': save_df.to_dict('records')
                })
                st.success(f"✅ 저장 완료! (ID: {doc_id})")
                time.sleep(1)
                st.rerun() 
            except Exception as e:
                st.error(f"저장 실패: {e}")
    st.markdown("---")

    if not df_uploaded.empty:
        # 1. 버짓 달성률
        st.subheader("📊 월별 버짓 달성 현황")
        BUDGET_DATA = {
            1:  {"rev": 514992575,  "rn": 2270, "adr": 226869, "occ": 56.3},
            2:  {"rev": 786570856,  "rn": 2577, "adr": 305227, "occ": 70.8},
            3:  {"rev": 529599040,  "rn": 2248, "adr": 235587, "occ": 55.8},
            4:  {"rev": 695351004,  "rn": 2414, "adr": 288049, "occ": 61.9},
            5:  {"rev": 903705440,  "rn": 3082, "adr": 293220, "occ": 76.5},
            6:  {"rev": 808203820,  "rn": 2776, "adr": 291140, "occ": 71.2},
            7:  {"rev": 1231949142, "rn": 3671, "adr": 335590, "occ": 91.1},
            8:  {"rev": 1388376999, "rn": 3873, "adr": 358476, "occ": 96.1},
            9:  {"rev": 952171506,  "rn": 2932, "adr": 324752, "occ": 75.2},
            10: {"rev": 897171539,  "rn": 3009, "adr": 298163, "occ": 74.7},
            11: {"rev": 667146771,  "rn": 2402, "adr": 277746, "occ": 61.6},
            12: {"rev": 804030110,  "rn": 2765, "adr": 290788, "occ": 68.6}
        }
        
        avail_months = sorted(df_view['date'].dt.month.unique())
        if not avail_months: avail_months = [datetime.now().month]
        
        sel_month = st.selectbox("월 선택 (1월, 2월...)", avail_months)
        m_data = df_view[df_view['date'].dt.month == sel_month]
        
        if not m_data.empty and sel_month in BUDGET_DATA:
            bg = BUDGET_DATA[sel_month]
            c_rev, c_rn = m_data['otb_revenue'].sum(), m_data['rooms_sold'].sum()
            c_adr = c_rev/c_rn if c_rn>0 else 0
            c_occ = m_data['occ'].mean()
            
            b1,b2,b3,b4 = st.columns(4)
            b1.metric("매출 달성률", f"{(c_rev/bg['rev'])*100:.1f}%", f"{int(c_rev):,}원")
            b2.metric("RN 달성률", f"{(c_rn/bg['rn'])*100:.1f}%", f"{int(c_rn):,}박")
            b3.metric("ADR", f"{int(c_adr):,}원", f"{int(c_adr-bg['adr']):,}원")
            b4.metric("OCC", f"{c_occ:.1f}%", f"목표 {bg['occ']}%")
        
        st.markdown("---")
        
        # 2. 마켓 레이더
        st.subheader("🧠 전략 인사이트")
        if '항공권' in df_view.columns and '렌터카' in df_view.columns:
            df_view['pressure'] = (df_view['항공권']*df_view['렌터카'])/((TOTAL_ROOMS-df_view['rooms_sold']).replace(0,1))
            if df_view['pressure'].max() > 0: df_view['pressure'] = (df_view['pressure']/df_view['pressure'].max())*100
            else: df_view['pressure'] = 0
        
        if '파르나스' in df_view.columns:
            df_view['comp_avg'] = df_view[['파르나스','그랜드조선']].mean(axis=1)
            df_view['leak'] = ((df_view['comp_avg']*0.85)-df_view['adr'])*df_view['rooms_sold']
            df_view['leak'] = df_view['leak'].apply(lambda x: x if x>0 else 0)
            
            if df_view['leak'].sum() > 0:
                leak_d = df_view.loc[df_view['leak'].idxmax()]
                st.error(f"🩸 {leak_d['date'].strftime('%m/%d')} 기회비용 경보: {int(leak_d['leak']):,}원 손실 예상")

        t1, t2, t3 = st.tabs(["손실 방어", "수요 파도", "시장 방어"])
        with t1: 
            if 'leak' in df_view.columns: st.plotly_chart(go.Figure([go.Bar(x=df_view['date'], y=df_view['otb_revenue'], name='매출'), go.Bar(x=df_view['date'], y=df_view['leak'], name='손실')]).update_layout(barmode='stack'), use_container_width=True)
        with t2:
            if '항공권' in df_view.columns: st.plotly_chart(make_subplots(specs=[[{"secondary_y":True}]]).add_trace(go.Scatter(x=df_view['date'], y=df_view['항공권'], name='항공'), secondary_y=False).add_trace(go.Scatter(x=df_view['date'], y=df_view['occ'], name='OCC'), secondary_y=True), use_container_width=True)
        with t3:
            if 'comp_avg' in df_view.columns: st.plotly_chart(px.scatter(df_view, x='comp_avg', y='adr', color='occ'), use_container_width=True)

        st.markdown("---")
        
        # 3. 심층 인사이트
        st.subheader("💡 데이터 심층 인사이트")
        t1, t2 = st.tabs(["💰 가격 민감도", "📅 요일별 패턴"])
        with t1:
            if not m_data.empty: st.plotly_chart(px.scatter(m_data, x='adr', y='occ', hover_data=['date'], title="ADR vs OCC 상관관계"), use_container_width=True)
        with t2:
            if not m_data.empty:
                d = m_data.copy()
                d['Day'] = d['date'].dt.day_name()
                st.plotly_chart(px.box(d, x='Day', y='otb_revenue', title="요일별 매출 분포", category_orders={"Day": ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']}), use_container_width=True)
        
        st.markdown("---")
        
        # 4. 수익 최적화 & ADR 워룸 
        st.subheader("💰 [ADR 워룸] 가격 결정 시뮬레이터")
        st.caption("현재 예약 속도(Pace)와 잔여 수요를 기반으로, 가격 변경 시의 최종 성과를 예측합니다.")
        
        def get_pace_rate(days_left):
            if days_left <= 0: return 1.0
            elif days_left <= 3: return 0.95
            elif days_left <= 7: return 0.85
            elif days_left <= 14: return 0.70
            elif days_left <= 30: return 0.50
            elif days_left <= 60: return 0.30
            else: return 0.15

        war1, war2 = st.columns([1, 2])
        
        with war1:
            target_date = st.date_input("타겟 날짜 선택", value=datetime.now().date() + timedelta(days=7), min_value=datetime.now().date())
            t_row = df_view[df_view['date'] == pd.to_datetime(target_date)]
            
            curr_sold = int(t_row['rooms_sold'].values[0]) if not t_row.empty else 0
            curr_rev = int(t_row['otb_revenue'].values[0]) if not t_row.empty else 0
            curr_adr_d = t_row['adr'].values[0] if not t_row.empty else 0
            
            parnas_p = t_row['파르나스'].values[0] if '파르나스' in t_row.columns and t_row['파르나스'].values[0] > 0 else 0
            josun_p = t_row['그랜드조선'].values[0] if '그랜드조선' in t_row.columns and t_row['그랜드조선'].values[0] > 0 else 0
            
            st.markdown(f"#### 📅 {target_date.strftime('%m/%d')} 현황")
            st.info(f"**현재 판매:** {curr_sold}실 / {TOTAL_ROOMS}실 (OCC {curr_sold/TOTAL_ROOMS*100:.1f}%)")
            st.write(f"**현재 ADR:** {int(curr_adr_d):,}원")
            
            st.markdown("---")
            st.markdown("#### ⚔️ 경쟁사 요금")
            if parnas_p > 0: st.write(f"- 파르나스: **{int(parnas_p):,}원** ({int(curr_adr_d - parnas_p):+,}원)")
            else: st.write("- 파르나스: 데이터 없음")
            
            if josun_p > 0: st.write(f"- 그랜드조선: **{int(josun_p):,}원** ({int(curr_adr_d - josun_p):+,}원)")
            else: st.write("- 그랜드조선: 데이터 없음")

        with war2:
            st.write("#### 🎚️ 시뮬레이션 설정")
            col_s1, col_s2 = st.columns(2)
            with col_s1: raise_amt = st.number_input("가격 조정 (+/- 원)", min_value=-200000, max_value=200000, value=-20000, step=5000)
            with col_s2: elasticity = st.select_slider("시장 민감도 (가격 반응도)", options=[0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 3.0], value=1.5, format_func=lambda x: f"{x}배 (매우 민감)" if x >= 2.0 else (f"{x}배 (둔감)" if x < 1 else "보통"))

            days_left = (pd.to_datetime(target_date) - datetime.now()).days
            pace_rate = get_pace_rate(days_left)
            final_forecast_sold_organic = min(TOTAL_ROOMS, int(curr_sold / pace_rate)) if pace_rate > 0 else curr_sold
            natural_pickup = max(0, final_forecast_sold_organic - curr_sold)
            new_adr = curr_adr_d + raise_amt
            price_change_pct = (raise_amt / curr_adr_d) if curr_adr_d > 0 else 0
            organic_impact = natural_pickup * (price_change_pct * -1 * elasticity)
            unsold_inventory = TOTAL_ROOMS - (curr_sold + natural_pickup)
            
            new_market_demand = (unsold_inventory * min(0.8, abs(price_change_pct) * elasticity * 2.0)) if raise_amt < 0 else 0
            adjusted_pickup = max(0, int(natural_pickup + organic_impact + new_market_demand))
            scenario_final_sold = min(TOTAL_ROOMS, curr_sold + adjusted_pickup)
            
            scenario_final_rev = (curr_sold * curr_adr_d) + (adjusted_pickup * new_adr)
            scenario_final_adr = scenario_final_rev / scenario_final_sold if scenario_final_sold > 0 else 0
            baseline_rev = final_forecast_sold_organic * curr_adr_d

            st.markdown("#### 🔮 예측 결과 비교")
            res_col1, res_col2 = st.columns(2)
            
            with res_col1:
                st.container(border=True).markdown(f"""
                **⏸️ 현상 유지 (Baseline)**
                - 예상 OCC: **{final_forecast_sold_organic}실** (+{natural_pickup})
                - 예상 매출: **{int(baseline_rev):,}원**
                - 예상 ADR: **{int(curr_adr_d):,}원**
                """)
            
            with res_col2:
                diff_rev = scenario_final_rev - baseline_rev
                diff_occ = scenario_final_sold - final_forecast_sold_organic
                st.container(border=True).markdown(f"""
                **🚀 가격 변경 시 ({raise_amt:+,}원)**
                - 예상 OCC: **{scenario_final_sold}실** ({diff_occ:+,}실)
                - 예상 매출: **{int(scenario_final_rev):,}원**
                - 예상 ADR: **{int(scenario_final_adr):,}원**
                """)

            if diff_rev > 0:
                if raise_amt < 0: st.success(f"🔥 **공격적 인하 성공:** 객실을 **{diff_occ}실** 더 채워 매출 **{int(diff_rev):+,}원** 이득입니다.")
                else: st.success(f"💰 **고수익 전략 성공:** ADR 상승 효과로 매출이 **{int(diff_rev):+,}원** 증가합니다.")
            else:
                st.error(f"📉 **전략 주의:** 매출이 **{int(diff_rev):+,}원** 감소할 위험이 있습니다.")

        with st.expander("⚔️ AI vs 총지배인 예측 배틀 (펼치기)"):
            with st.form("battle_v6"):
                ai_pred = int(curr_rev / 0.8) if curr_rev > 0 else 0
                st.write(f"🤖 AI 마감 예측: **{ai_pred:,} 원**")
                gm_input = st.number_input("내 예측 금액 (원)", value=ai_pred, step=100000)
                if st.form_submit_button("⚔️ 승부수 던지기"): st.success(f"저장 완료!")

        st.markdown("---")
        
        # 5. 180일 종합 추세 
        st.subheader("📊 180일 종합 추세 & 스냅샷 비교")
        
        try:
            snapshot_list = sorted([doc.id for doc in db_flight.collection('strategy_history').stream()], reverse=True)
        except: snapshot_list = [] 

        compare_date = st.selectbox("⏳ 과거 저장 시점과 비교하기 (스냅샷 비교)", ["비교 안 함"] + snapshot_list)
        
        df_past = pd.DataFrame()
        if compare_date != "비교 안 함":
            try:
                doc = db_flight.collection('strategy_history').document(compare_date).get()
                if doc.exists:
                    df_past = pd.DataFrame(doc.to_dict().get('data', []))
                    if not df_past.empty:
                        df_past['date'] = pd.to_datetime(df_past['date'])
                        st.info(f"🕰️ **{compare_date}** 시점의 예측 데이터(점선)와 현재(실선)를 비교합니다.")
            except Exception as e: st.error(f"과거 데이터 로드 실패: {e}")

        v_tab1, v_tab2 = st.tabs(["💰 정규화 추세 (Normalized)", "📈 가격 격차 (Price Gap)"])
        with v_tab1:
            df_chart = df_view.copy()
            comp_list = [c for c in ['파르나스', '그랜드조선'] if c in df_chart.columns]
            if comp_list: df_chart['경쟁사'] = df_chart[comp_list].mean(axis=1)
            valid_cols = [c for c in ['otb_revenue', '항공권', '경쟁사', '렌터카'] if c in df_chart.columns]
            
            if len(valid_cols) > 1:
                from sklearn.preprocessing import MinMaxScaler
                df_norm = df_chart.dropna(subset=valid_cols).copy()
                if not df_norm.empty:
                    df_norm[valid_cols] = MinMaxScaler().fit_transform(df_norm[valid_cols])
                    st.plotly_chart(px.line(df_norm.melt(id_vars=['date'], value_vars=valid_cols), x='date', y='value', color='variable', line_shape='spline'), use_container_width=True)

        with v_tab2:
            fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
            if '파르나스' in df_view.columns: fig_dual.add_trace(go.Scatter(x=df_view['date'], y=df_view['파르나스'], name='파르나스', line=dict(color='green', width=1.5)), secondary_y=True)
            if '그랜드조선' in df_view.columns: fig_dual.add_trace(go.Scatter(x=df_view['date'], y=df_view['그랜드조선'], name='그랜드조선', line=dict(color='blue', width=1.5)), secondary_y=True)
            if '엠버트윈' in df_view.columns: fig_dual.add_trace(go.Scatter(x=df_view['date'], y=df_view['엠버트윈'], name='엠버트윈', line=dict(color='orange', width=2, dash='dot')), secondary_y=True)
            fig_dual.add_trace(go.Scatter(x=df_view['date'], y=df_view['otb_revenue'], name='우리 매출(현재)', line=dict(color='red', width=3)), secondary_y=False)
            
            if not df_past.empty and 'otb_revenue' in df_past.columns:
                fig_dual.add_trace(go.Scatter(x=df_past['date'], y=df_past['otb_revenue'], name=f'매출({compare_date})', line=dict(color='gray', width=2, dash='dash')), secondary_y=False)
            
            fig_dual.update_layout(title="매출(좌) vs 경쟁사 가격(우) 이중축 비교", height=500, yaxis2=dict(title="가격(원)", overlaying='y', side='right'))
            st.plotly_chart(fig_dual, use_container_width=True)
        
        st.download_button("📥 통합 데이터 다운로드", df_view.to_csv().encode('utf-8-sig'), "data.csv")

    else:
        st.info("💡 좌측 패널에서 온북 엑셀 파일을 업로드하시면 엠버 OTB 및 전체 전략 분석이 활성화됩니다.")
        
# =========================================================================
# ===== [이식 영역] 원래 '대시보드.py' 파일의 3개 탭 전체 =====
# 독립된 load_hotel_comp_data_legacy() 로더를 사용하여 기능 100% 보존
# =========================================================================
df_legacy = load_hotel_comp_data_legacy()

# --- TAB 8: [원 대시보드.py] 시장 최저가 대시보드 ---
with tab8:
    st.title("🏨 Jeju Hotel Market Intelligence")
    st.markdown(f"**엠버퓨어힐 전략 상황실** | *Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M')}*")

    if df_legacy.empty:
        st.warning("⚠️ 아직 수집된 데이터가 없습니다. 'hotel_spy.py'를 먼저 실행해서 데이터를 모아주세요.")
    else:
        # 가장 최근에 조사한 데이터만 필터링 (현재 시장가)
        latest_search_date = df_legacy["Search_Date"].max()
        current_market_df = df_legacy[df_legacy["Search_Date"] == latest_search_date].copy()

        # 상단 요약 메트릭
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        
        # 엠버 평균 최저가 계산
        amber_avg = current_market_df[current_market_df["Hotel"]=="Amber_Pure_Hill"]["Min_Price"].mean()
        parnas_avg = current_market_df[current_market_df["Hotel"]=="Parnas_Jeju"]["Min_Price"].mean()
        josun_avg = current_market_df[current_market_df["Hotel"]=="Grand_Josun"]["Min_Price"].mean()

        col1.metric("엠버퓨어힐 평균가", f"{int(amber_avg):,}원" if pd.notnull(amber_avg) else "-")
        col2.metric("파르나스 제주 평균가", f"{int(parnas_avg):,}원" if pd.notnull(parnas_avg) else "-", 
                    delta=f"{int(amber_avg - parnas_avg):,}원" if pd.notnull(parnas_avg) and pd.notnull(amber_avg) else None, delta_color="inverse")
        col3.metric("그랜드조선 평균가", f"{int(josun_avg):,}원" if pd.notnull(josun_avg) else "-",
                    delta=f"{int(amber_avg - josun_avg):,}원" if pd.notnull(josun_avg) and pd.notnull(amber_avg) else None, delta_color="inverse")
        col4.metric("데이터 기준일", f"{latest_search_date} (Latest)")

        st.markdown("---")

        st.subheader(f"📅 일별 최저가 흐름 (기준: {latest_search_date} 조사)")
        
        # 선 그래프 그리기
        fig_comp = px.line(current_market_df, 
                           x="CheckIn_Date", 
                           y="Min_Price", 
                           color="Hotel",
                           markers=True,
                           color_discrete_map={
                               "Amber_Pure_Hill": "#FF4B4B",  # 엠버는 강조색(Red)
                               "Parnas_Jeju": "#1f77b4",      # 파르나스(Blue)
                               "Grand_Josun": "#2ca02c"       # 조선(Green)
                           },
                           hover_data={"Min_Price": ":,.0f"})
        
        fig_comp.update_layout(height=500, xaxis_title="체크인 날짜", yaxis_title="최저가 (원)")
        st.plotly_chart(fig_comp, use_container_width=True)

# --- TAB 9: [원 대시보드.py] 엠버 객실 전략 ---
with tab9:
    st.subheader("💎 엠버퓨어힐 객실 등급별 가격 Gap 분석")
    
    if df_legacy.empty:
        st.warning("⚠️ 아직 수집된 데이터가 없습니다.")
    else:
        latest_search_date = df_legacy["Search_Date"].max()
        current_market_df = df_legacy[df_legacy["Search_Date"] == latest_search_date].copy()
        amber_data = current_market_df[current_market_df["Hotel"] == "Amber_Pure_Hill"].copy()
        
        if amber_data.empty:
            st.info("엠버퓨어힐 데이터가 없습니다.")
        else:
            # 그래프용 데이터 구조 변경 (Melt)
            # 최저가, 힐엠버트윈, 힐파인더블을 하나의 컬럼으로 합침
            amber_melted = amber_data.melt(id_vars=["CheckIn_Date"], 
                                           value_vars=["Min_Price", "Amber_Twin", "Pine_Double"],
                                           var_name="Room_Type", value_name="Price")
            
            # 결측치 제거 (가격 없는 날 제외)
            amber_melted = amber_melted.dropna(subset=["Price"])

            fig_internal = px.line(amber_melted, 
                                   x="CheckIn_Date", 
                                   y="Price", 
                                   color="Room_Type",
                                   markers=True,
                                   line_dash="Room_Type", # 선 스타일 다르게
                                   title="객실 타입별 가격 차이 (Spread)",
                                   color_discrete_map={
                                       "Min_Price": "gray",
                                       "Amber_Twin": "#FFA15A",
                                       "Pine_Double": "#EF553B"
                                   })
            
            fig_internal.update_traces(hovertemplate='%{y:,.0f}원')
            fig_internal.update_layout(height=500, xaxis_title="체크인 날짜", yaxis_title="판매가 (원)")
            st.plotly_chart(fig_internal, use_container_width=True)
            
            # [전략 인사이트] Gap 통계 보여주기
            st.markdown("#### 💡 Strategy Insight: Price Gap")
            gap_col1, gap_col2 = st.columns(2)
            
            # 평균 Gap 계산
            amber_data["Gap_Twin"] = amber_data["Amber_Twin"] - amber_data["Min_Price"]
            amber_data["Gap_Double"] = amber_data["Pine_Double"] - amber_data["Min_Price"]
            
            avg_gap_twin = amber_data["Gap_Twin"].mean()
            avg_gap_double = amber_data["Gap_Double"].mean()
            
            gap_col1.info(f"**힐 엠버 트윈 프리미엄:** 평균 +{int(avg_gap_twin):,}원 (vs 최저가)" if pd.notnull(avg_gap_twin) else "**힐 엠버 트윈 프리미엄:** 데이터 없음")
            gap_col2.success(f"**힐 파인 더블 프리미엄:** 평균 +{int(avg_gap_double):,}원 (vs 최저가)" if pd.notnull(avg_gap_double) else "**힐 파인 더블 프리미엄:** 데이터 없음")

# --- TAB 10: [원 대시보드.py] 가격 변동 추이 (Time Travel) ---
with tab10:
    st.subheader("📈 특정 날짜의 가격 변동 이력 (언제 가격이 올랐나?)")
    
    if df_legacy.empty:
        st.warning("⚠️ 아직 수집된 데이터가 없습니다.")
    else:
        # 사용자가 분석하고 싶은 '체크인 날짜' 선택
        available_dates = sorted(df_legacy["CheckIn_Date"].unique())
        selected_date = st.selectbox("체크인 날짜를 선택하세요:", available_dates, format_func=lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
        
        # 선택한 날짜의 데이터만 필터링
        history_df = df_legacy[df_legacy["CheckIn_Date"] == selected_date].copy()
        
        if history_df.empty:
            st.write("선택한 날짜의 기록이 없습니다.")
        else:
            # x축을 '조사한 날짜(Search_Date)'로 설정하여 시계열 변화 확인
            fig_history = go.Figure()

            # 1. 엠버 최저가
            amber_hist = history_df[history_df["Hotel"]=="Amber_Pure_Hill"]
            fig_history.add_trace(go.Scatter(x=amber_hist["Search_Date_DT"], y=amber_hist["Min_Price"], 
                                             mode='lines+markers', name='엠버 최저가', line=dict(color='red')))
            
            # 2. 엠버 트윈/더블
            fig_history.add_trace(go.Scatter(x=amber_hist["Search_Date_DT"], y=amber_hist["Amber_Twin"], 
                                             mode='lines+markers', name='힐 엠버 트윈', line=dict(color='orange', dash='dot')))
            
            # 3. 파르나스
            parnas_hist = history_df[history_df["Hotel"]=="Parnas_Jeju"]
            fig_history.add_trace(go.Scatter(x=parnas_hist["Search_Date_DT"], y=parnas_hist["Min_Price"], 
                                             mode='lines+markers', name='파르나스', line=dict(color='blue')))
            
            # 4. 조선
            josun_hist = history_df[history_df["Hotel"]=="Grand_Josun"]
            fig_history.add_trace(go.Scatter(x=josun_hist["Search_Date_DT"], y=josun_hist["Min_Price"], 
                                             mode='lines+markers', name='조선', line=dict(color='green')))

            fig_history.update_layout(title=f"{pd.to_datetime(selected_date).strftime('%Y-%m-%d')} 숙박 요금의 변화 추이",
                                      xaxis_title="데이터 수집일 (Search Date)",
                                      yaxis_title="가격 (원)",
                                      hovermode="x unified")
            st.plotly_chart(fig_history, use_container_width=True)
            
            st.caption("※ X축은 데이터를 수집한 날짜입니다. 그래프가 우상향하면 가격이 오르고 있다는 뜻입니다.")
