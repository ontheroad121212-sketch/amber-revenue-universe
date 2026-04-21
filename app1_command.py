"""
🏨 Amber Command Center v8.0
엠버퓨어힐 통합 수익관리 시스템

[8단계] 5가지 대규모 업데이트
1. 과거 날짜 알림 제외 (오늘 이후 미래만)
2. PDF 한글 폰트 지원 (NanumGothic.ttf 필요)
3. 📅 주간 보고서 자동 요약
4. 🎯 할 일 체크리스트
5. 📊 전년 동기 비교
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import firebase_admin
from firebase_admin import credentials, firestore
import math
import re
import io
import os
import plotly.express as px
import plotly.graph_objects as go
from fpdf import FPDF
import calendar


# ============================================================
# 2. 비밀번호
# ============================================================
def check_password():
    if "cmd_authenticated" not in st.session_state:
        st.session_state["cmd_authenticated"] = False
    if not st.session_state["cmd_authenticated"]:
        st.title("🔐 Amber Command Center")
        st.caption("관계자 외 출입 금지")
        pw = st.text_input("접속 암호", type="password")
        if st.button("접속"):
            if pw == "0822":
                st.session_state["cmd_authenticated"] = True
                st.rerun()
            else:
                st.error("암호가 틀렸습니다.")
        return False
    return True

# ============================================================
# 3. Firebase 초기화
# ============================================================
existing_apps = [a.name for a in firebase_admin._apps.values()] if firebase_admin._apps else []

if "hotel_app" not in existing_apps:
    try:
        cred_h = credentials.Certificate(dict(st.secrets["firebase"]))
        firebase_admin.initialize_app(cred_h, name="hotel_app")
    except Exception as e:
        st.error(f"호텔 Firebase 연결 실패: {e}")
        st.stop()

if "flight_app" not in existing_apps:
    try:
        cred_f = credentials.Certificate(dict(st.secrets["firebase_flight"]))
        firebase_admin.initialize_app(cred_f, name="flight_app")
    except Exception as e:
        st.warning(f"항공 Firebase 연결 실패: {e}")

# ===== 여기에 추가! =====
try:
    db_hotel = firestore.client(app=firebase_admin.get_app("hotel_app"))
except:
    st.error("호텔 DB 연결 실패")
    db_hotel = None  # ← None으로라도 정의해야 함!

try:
    db_flight = firestore.client(app=firebase_admin.get_app("flight_app"))
except:
    db_flight = None


# ============================================================
# 4. 오늘 날짜 (한국 기준)
# ============================================================
TODAY = date.today()

# ============================================================
# 5. 전역 설정
# ============================================================
BAR_GRADIENT_COLORS = {
    "BAR0": "#B71C1C", "BAR1": "#D32F2F", "BAR2": "#EF5350", "BAR3": "#FF8A65",
    "BAR4": "#FFB199", "BAR5": "#81C784", "BAR6": "#A5D6A7", "BAR7": "#C8E6C9", "BAR8": "#E8F5E9",
}
BAR_LIGHT_COLORS = {
    "BAR0": "#FFCDD2", "BAR1": "#FFEBEE", "BAR2": "#FFEBEE", "BAR3": "#FFF3E0",
    "BAR4": "#FFF3E0", "BAR5": "#E8F5E9", "BAR6": "#E8F5E9", "BAR7": "#F1F8E9", "BAR8": "#F1F8E9",
}
SIM_BAR_COLORS = {
    "BAR0": "#0D47A1", "BAR1": "#1565C0", "BAR2": "#1976D2", "BAR3": "#42A5F5",
    "BAR4": "#90CAF9", "BAR5": "#26A69A", "BAR6": "#4DB6AC", "BAR7": "#80CBC4", "BAR8": "#B2DFDB",
}

PAST_COLOR = "#E0E0E0"

WEEKDAYS_KR = ['월', '화', '수', '목', '금', '토', '일']
DYNAMIC_ROOMS = ["FDB", "FDE", "HDP", "HDT", "HDF"]
FIXED_ROOMS = ["GDB", "GDF", "FFD", "FPT", "PPV"]
ALL_ROOMS = DYNAMIC_ROOMS + FIXED_ROOMS
BAR_LEVELS = ["BAR8", "BAR7", "BAR6", "BAR5", "BAR4", "BAR3", "BAR2", "BAR1", "BAR0"]

DEFAULT_SENSITIVITY = {
    "FDB": 1.0, "FDE": 1.0, "HDP": 1.2, "HDT": 1.3, "HDF": 0.7,
}

PRICE_TABLE = {
    "FDB": {"BAR0": 802000, "BAR8": 315000, "BAR7": 353000, "BAR6": 396000, "BAR5": 445000, "BAR4": 502000, "BAR3": 567000, "BAR2": 642000, "BAR1": 728000},
    "FDE": {"BAR0": 839000, "BAR8": 352000, "BAR7": 390000, "BAR6": 433000, "BAR5": 482000, "BAR4": 539000, "BAR3": 604000, "BAR2": 679000, "BAR1": 765000},
    "HDP": {"BAR0": 759000, "BAR8": 280000, "BAR7": 318000, "BAR6": 361000, "BAR5": 410000, "BAR4": 467000, "BAR3": 532000, "BAR2": 607000, "BAR1": 693000},
    "HDT": {"BAR0": 729000, "BAR8": 250000, "BAR7": 288000, "BAR6": 331000, "BAR5": 380000, "BAR4": 437000, "BAR3": 502000, "BAR2": 577000, "BAR1": 663000},
    "HDF": {"BAR0": 916000, "BAR8": 420000, "BAR7": 458000, "BAR6": 501000, "BAR5": 550000, "BAR4": 607000, "BAR3": 672000, "BAR2": 747000, "BAR1": 833000},
}
FIXED_PRICE_TABLE = {
    "GDB": {"UND1": 298000, "UND2": 298000, "MID1": 298000, "MID2": 298000, "UPP1": 298000, "UPP2": 298000},
    "GDF": {"UND1": 375000, "UND2": 410000, "MID1": 410000, "MID2": 488000, "UPP1": 488000, "UPP2": 578000},
    "FFD": {"UND1": 353000, "UND2": 393000, "MID1": 433000, "MID2": 482000, "UPP1": 539000, "UPP2": 604000},
    "FPT": {"UND1": 500000, "UND2": 550000, "MID1": 600000, "MID2": 650000, "UPP1": 700000, "UPP2": 750000},
    "PPV": {"UND1": 1104000, "UND2": 1154000, "MID1": 1154000, "MID2": 1304000, "UPP1": 1304000, "UPP2": 1554000},
}
FIXED_BAR0_TABLE = {"GDB": 298000, "GDF": 678000, "FFD": 704000, "FPT": 850000, "PPV": 1704000}

# ============================================================
# 6. 로직 함수
# ============================================================
@st.cache_data(ttl=60)
def load_applied_rates():
    try:
        if not db_hotel: return {}
        docs = db_hotel.collection("applied_rates").stream()
        result = {}
        for doc in docs:
            result[doc.id] = doc.to_dict()
        return result
    except Exception:
        return {}

def get_season_details(date_obj):
    m, d = date_obj.month, date_obj.day
    md = f"{m:02d}.{d:02d}"
    actual_is_weekend = date_obj.weekday() in [4, 5]
    if ("02.13" <= md <= "02.18") or ("09.23" <= md <= "09.28"):
        season, is_weekend = "UPP", True
    elif ("12.21" <= md <= "12.31") or ("10.01" <= md <= "10.08"):
        season, is_weekend = "UPP", False
    elif ("05.03" <= md <= "05.05") or ("05.24" <= md <= "05.26") or ("06.05" <= md <= "06.07"):
        season, is_weekend = "MID", True
    elif "07.17" <= md <= "08.29":
        season, is_weekend = "UPP", actual_is_weekend
    elif ("01.04" <= md <= "03.31") or ("11.01" <= md <= "12.20"):
        season, is_weekend = "UND", actual_is_weekend
    else:
        season, is_weekend = "MID", actual_is_weekend
    type_code = f"{season}{'2' if is_weekend else '1'}"
    return type_code, season, is_weekend

def determine_bar(season, is_weekend, occ):
    if season == "UPP":
        if is_weekend:
            if occ >= 81: return "BAR1"
            elif occ >= 51: return "BAR2"
            elif occ >= 31: return "BAR3"
            else: return "BAR4"
        else:
            if occ >= 81: return "BAR2"
            elif occ >= 51: return "BAR3"
            elif occ >= 31: return "BAR4"
            else: return "BAR5"
    elif season == "MID":
        if is_weekend:
            if occ >= 81: return "BAR3"
            elif occ >= 51: return "BAR4"
            elif occ >= 31: return "BAR5"
            else: return "BAR6"
        else:
            if occ >= 81: return "BAR4"
            elif occ >= 51: return "BAR5"
            elif occ >= 31: return "BAR6"
            else: return "BAR7"
    else:
        if is_weekend:
            if occ >= 81: return "BAR4"
            elif occ >= 51: return "BAR5"
            elif occ >= 31: return "BAR6"
            else: return "BAR7"
        else:
            if occ >= 81: return "BAR5"
            elif occ >= 51: return "BAR6"
            elif occ >= 31: return "BAR7"
            else: return "BAR8"

def bar_to_index(bar_str):
    try: return BAR_LEVELS.index(bar_str)
    except: return 0

def index_to_bar(idx):
    idx = max(0, min(idx, len(BAR_LEVELS) - 1))
    return BAR_LEVELS[idx]

def get_final_values(room_id, date_obj, avail, total, manual_bar=None):
    type_code, season, is_weekend = get_season_details(date_obj)
    try:
        current_avail = float(avail) if pd.notna(avail) else 0.0
    except:
        current_avail = 0.0
    occ = ((total - current_avail) / total * 100) if total > 0 else 0

    # [핵심 수정] 다른 사이트에서 덮어씌운 '수동 확정 요금'이 있는지 체크해서 무조건 1순위로 적용
    if not manual_bar:
        applied_rates = load_applied_rates()
        date_str = date_obj.strftime('%Y-%m-%d')
        manual_bar = applied_rates.get(date_str, {}).get('rooms', {}).get(room_id)

    if manual_bar:
        bar = manual_bar
        if bar == "BAR0":
            price = PRICE_TABLE.get(room_id, {}).get("BAR0", 0) if room_id in DYNAMIC_ROOMS else FIXED_BAR0_TABLE.get(room_id, 0)
        else:
            price = PRICE_TABLE.get(room_id, {}).get(bar, 0) if room_id in DYNAMIC_ROOMS else FIXED_PRICE_TABLE.get(room_id, {}).get(bar, 0)
        return occ, bar, price, True

    if room_id in DYNAMIC_ROOMS:
        bar = determine_bar(season, is_weekend, occ)
        price = PRICE_TABLE.get(room_id, {}).get(bar, 0)
    else:
        bar = type_code
        price = FIXED_PRICE_TABLE.get(room_id, {}).get(type_code, 0)
    return occ, bar, price, False
    
# ============================================================
# 7. 📝 메모 시스템 (completed 필드 추가)
# ============================================================
@st.cache_data(ttl=60)
def load_all_notes():
    try:
        if not db_hotel: return {}
        docs = db_hotel.collection("notes").stream()
        notes = {}
        for doc in docs:
            d = doc.to_dict()
            notes[doc.id] = {
                'content': d.get('content', ''),
                'tag': d.get('tag', '일반'),
                'updated_at': d.get('updated_at', ''),
                'completed': d.get('completed', False),
            }
        return notes
    except:
        return {}
 
def save_note(key, content, tag="일반", completed=False):
    try:
        if not db_hotel: return False
        if content.strip():
            db_hotel.collection("notes").document(key).set({
                'content': content, 'tag': tag,
                'updated_at': datetime.now().isoformat(),
                'completed': completed,
            })
        else:
            db_hotel.collection("notes").document(key).delete()
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"메모 저장 실패: {e}")
        return False
 
def toggle_note_completed(key, completed):
    try:
        if not db_hotel: return False
        doc_ref = db_hotel.collection("notes").document(key)
        doc = doc_ref.get()
        if doc.exists:
            data = doc.to_dict()
            data['completed'] = completed
            doc_ref.set(data)
            st.cache_data.clear()
            return True
    except:
        return False
 
# ============================================================
# 8. 🗓️ 이벤트
# ============================================================
@st.cache_data(ttl=60)
def load_events():
    try:
        if not db_hotel: return []
        docs = db_hotel.collection("events").stream()
        events = []
        for doc in docs:
            d = doc.to_dict()
            events.append({
                'id': doc.id, 'name': d.get('name', ''),
                'start_date': d.get('start_date', ''), 'end_date': d.get('end_date', ''),
                'impact': d.get('impact', 1), 'note': d.get('note', ''),
            })
        return events
    except:
        return []
 
def save_event(event_id, name, start_d, end_d, impact, note):
    try:
        if not db_hotel: return False
        db_hotel.collection("events").document(event_id).set({
            'name': name, 'start_date': start_d.strftime('%Y-%m-%d'),
            'end_date': end_d.strftime('%Y-%m-%d'), 'impact': impact, 'note': note,
        })
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"이벤트 저장 실패: {e}")
        return False
 
def delete_event(event_id):
    try:
        if not db_hotel: return False
        db_hotel.collection("events").document(event_id).delete()
        st.cache_data.clear()
        return True
    except:
        return False

# ============================================================
# 9. 🎯 민감도
# ============================================================
@st.cache_data(ttl=300)
def load_sensitivity():
    try:
        if not db_hotel: return DEFAULT_SENSITIVITY.copy()
        doc = db_hotel.collection("settings").document("sensitivity").get()
        if doc.exists:
            return doc.to_dict()
    except:
        pass
    return DEFAULT_SENSITIVITY.copy()
 
def save_sensitivity(sens_dict):
    try:
        if not db_hotel: return False
        db_hotel.collection("settings").document("sensitivity").set(sens_dict)
        st.cache_data.clear()
        return True
    except:
        return False

# ============================================================
# 10. 시뮬레이터
# ============================================================
def get_sim_bar(room_id, date_obj, avail, total,
                josun_price, flight_price, parnas_price,
                josun_threshold, flight_threshold,
                josun_prev_price=None, events=None, sensitivity=None):
    occ, sys_bar, sys_price, _ = get_final_values(room_id, date_obj, avail, total)

    if room_id not in DYNAMIC_ROOMS:
        return occ, sys_bar, sys_price, 0, "고정요금"

    sys_idx = bar_to_index(sys_bar)
    base_boost = 0
    signals = []

    if josun_price and josun_price >= josun_threshold:
        base_boost += 1
        signals.append(f"조선 {int(josun_price):,}↑")

    if flight_price and flight_price >= flight_threshold:
        base_boost += 1
        signals.append(f"항공 {int(flight_price):,}↑")

    if josun_prev_price and josun_price:
        try:
            drop_rate = (josun_prev_price - josun_price) / josun_prev_price
            if drop_rate >= 0.15:
                base_boost = 0
                signals.append(f"조선급락방어({int(drop_rate*100)}%↓)")
        except:
            pass

    if events:
        event_boost, event_names = get_event_boost_for_date(date_obj, events)
        if event_boost > 0:
            base_boost += event_boost
            signals.append(f"이벤트({', '.join(event_names)}) +{event_boost}")

    if sensitivity and room_id in sensitivity:
        sens_factor = sensitivity[room_id]
        final_boost = round(base_boost * sens_factor)
        if sens_factor != 1.0 and base_boost > 0:
            signals.append(f"민감도×{sens_factor}")
    else:
        final_boost = base_boost

    sim_idx = min(sys_idx + final_boost, len(BAR_LEVELS) - 2)
    sim_bar = index_to_bar(sim_idx)
    sim_price = PRICE_TABLE.get(room_id, {}).get(sim_bar, sys_price)

    signal_str = " + ".join(signals) if signals else "기본"
    return occ, sim_bar, sim_price, final_boost, signal_str

# ============================================================
# 11. 🚨 Fixed 수기 인상 알림 (과거/미래 분리)
# ============================================================
@st.cache_data(ttl=600, show_spinner=False) # ✨ 이 한 줄만 추가하세요!
def get_fixed_room_alerts(current_df, events=None):
    if current_df.empty:
        return [], []

    alerts_future = []
    alerts_past = []

    dates = sorted(current_df['Date'].unique())

    for d in dates:
        day_data = current_df[current_df['Date'] == d]
        total_avail = day_data['Available'].sum()
        total_rooms = day_data['Total'].sum()

        if total_rooms == 0:
            continue

        overall_occ = ((total_rooms - total_avail) / total_rooms * 100)
        _, event_names = get_event_boost_for_date(d, events or [])
        has_event = len(event_names) > 0
        is_past = d < TODAY

        if overall_occ >= 85:
            alert = {
                '날짜': d, '요일': WEEKDAYS_KR[d.weekday()],
                '전체점유율': round(overall_occ, 1),
                '단계': '🚨 강력 권장', '메시지': 'Fixed 객실 수기 인상 긴급',
                '이벤트': ', '.join(event_names) if has_event else '',
                'level': 'critical', 'is_past': is_past
            }
            if is_past:
                alerts_past.append(alert)
            else:
                alerts_future.append(alert)
        elif overall_occ >= 75:
            alert = {
                '날짜': d, '요일': WEEKDAYS_KR[d.weekday()],
                '전체점유율': round(overall_occ, 1),
                '단계': '🔔 검토 권장', '메시지': 'Fixed 객실 수기 인상 검토',
                '이벤트': ', '.join(event_names) if has_event else '',
                'level': 'warning', 'is_past': is_past
            }
            if is_past:
                alerts_past.append(alert)
            else:
                alerts_future.append(alert)

    return alerts_future, alerts_past

# ============================================================
# 12. 시장 데이터
# ============================================================
@st.cache_data(ttl=3600)
def load_market_data():
    df_flight = pd.DataFrame()
    df_comp = pd.DataFrame()
    if db_flight is None:
        return df_flight, df_comp
    try:
        flight_docs = db_flight.collection('flight_prices').stream()
        flight_list = [{
            'date': d.to_dict().get('date', ''),
            'min_price': d.to_dict().get('min_price', 0),
            'search_date_str': d.to_dict().get('search_date_str', '')
        } for d in flight_docs]
        df_flight = pd.DataFrame(flight_list)
        if not df_flight.empty:
            df_flight['date'] = pd.to_datetime(df_flight['date'], errors='coerce').dt.date
    except:
        pass
    try:
        comp_docs = db_flight.collection('hotel_comp_prices').stream()
        comp_list = [{
            'date': d.to_dict().get('date', ''),
            'hotel_name': d.to_dict().get('hotel_name', ''),
            'price': d.to_dict().get('price', 0),
            'search_date_str': d.to_dict().get('search_date_str', '')
        } for d in comp_docs]
        df_comp = pd.DataFrame(comp_list)
        if not df_comp.empty:
            df_comp['date'] = pd.to_datetime(df_comp['date'], errors='coerce').dt.date
    except:
        pass
    return df_flight, df_comp

def get_market_price_for_date(target_date, df_flight, df_comp, search_date_str=None):
    josun_price, parnas_price, flight_price = None, None, None
    if not df_flight.empty:
        f_row = df_flight[df_flight['date'] == target_date]
        if search_date_str and 'search_date_str' in f_row.columns:
            f_row = f_row[f_row['search_date_str'] == search_date_str]
        if not f_row.empty:
            flight_price = f_row['min_price'].min()
    if not df_comp.empty:
        c_row = df_comp[df_comp['date'] == target_date]
        if search_date_str and 'search_date_str' in c_row.columns:
            c_row = c_row[c_row['search_date_str'] == search_date_str]
        josun_rows = c_row[c_row['hotel_name'].str.contains('Josun|조선', case=False, na=False)]
        parnas_rows = c_row[c_row['hotel_name'].str.contains('Parnas|파르나스', case=False, na=False)]
        if not josun_rows.empty:
            josun_price = josun_rows['price'].min()
        if not parnas_rows.empty:
            parnas_price = parnas_rows['price'].min()
    return josun_price, parnas_price, flight_price

# ============================================================
# 13. 기회비용 계산 (is_past 필드 추가)
# ============================================================
@st.cache_data(ttl=600, show_spinner=False) # ✨ 이 한 줄만 추가하세요!
def calculate_opportunity_cost(current_df, df_flight, df_comp,
                                josun_threshold, flight_threshold,
                                search_date_str=None, events=None, sensitivity=None):
    records = []
    dates = sorted(current_df['Date'].unique())
    for d in dates:
        josun_p, parnas_p, flight_p = get_market_price_for_date(d, df_flight, df_comp, search_date_str)
        prev_week_date = d - timedelta(days=7)
        josun_prev, _, _ = get_market_price_for_date(prev_week_date, df_flight, df_comp, search_date_str)
        for rid in DYNAMIC_ROOMS:
            curr_match = current_df[(current_df['RoomID'] == rid) & (current_df['Date'] == d)]
            if curr_match.empty:
                continue
            avail = curr_match.iloc[0]['Available']
            total = curr_match.iloc[0]['Total']
            occ, real_bar, real_price, _ = get_final_values(rid, d, avail, total)
            _, sim_bar, sim_price, boost, signal_str = get_sim_bar(
                rid, d, avail, total,
                josun_p, flight_p, parnas_p,
                josun_threshold, flight_threshold,
                josun_prev_price=josun_prev, events=events, sensitivity=sensitivity
            )
            try:
                sold_rooms = max(0, float(total) - (float(avail) if pd.notna(avail) else 0))
            except:
                sold_rooms = 0
            price_diff = sim_price - real_price
            opp_cost = price_diff * sold_rooms
            records.append({
                '날짜': d, '요일': WEEKDAYS_KR[d.weekday()], '객실타입': rid,
                '점유율(%)': round(occ, 1), '판매객실수': int(sold_rooms),
                '실제BAR': real_bar, '실제단가': int(real_price),
                '시뮬BAR': sim_bar, '시뮬단가': int(sim_price),
                '단가차이': int(price_diff), '기회비용': int(opp_cost),
                'BAR상승': boost, '시그널': signal_str,
                'is_past': d < TODAY,
            })
    return pd.DataFrame(records)

def get_our_avg_price_for_dates(current_df, target_dates):
    result = {}
    for d in target_dates:
        prices = []
        for rid in DYNAMIC_ROOMS:
            curr_match = current_df[(current_df['RoomID'] == rid) & (current_df['Date'] == d)]
            if curr_match.empty:
                continue
            avail = curr_match.iloc[0]['Available']
            total = curr_match.iloc[0]['Total']
            _, _, price, _ = get_final_values(rid, d, avail, total)
            if price > 0:
                prices.append(price)
        result[d] = sum(prices) / len(prices) if prices else None
    return result

# ============================================================
# 14. 🚨 오늘 알림 (미래만)
# ============================================================
def generate_today_alerts(opp_df, notes, events, top_n=5, only_future=True):
    if opp_df.empty:
        return []
    triggered = opp_df[opp_df['BAR상승'] > 0].copy()
    if triggered.empty:
        return []

    if only_future:
        triggered = triggered[triggered['날짜'] >= TODAY]

    if triggered.empty:
        return []

    daily = triggered.groupby('날짜').agg({
        '기회비용': 'sum', '시그널': lambda x: x.iloc[0], 'BAR상승': 'max'
    }).reset_index()
    daily = daily.sort_values('기회비용', ascending=False).head(top_n)
    alerts = []
    for _, row in daily.iterrows():
        d = row['날짜']
        day_note_key = get_note_key(d)
        has_note = day_note_key in notes
        note_preview = notes[day_note_key]['content'][:30] if has_note else ""
        _, event_names = get_event_boost_for_date(d, events)
        alerts.append({
            '날짜': d, '요일': WEEKDAYS_KR[d.weekday()],
            'BAR상승': int(row['BAR상승']), '시그널': row['시그널'],
            '기회비용': int(row['기회비용']),
            '메모있음': has_note, '메모미리보기': note_preview,
            '이벤트': ', '.join(event_names) if event_names else '',
        })
    return alerts

# ============================================================
# 15. 📅 주간 요약 생성
# ============================================================
def generate_weekly_summary(opp_df, notes, events):
    """지난 주 / 이번 주 / 다음 주 요약"""
    today_weekday = TODAY.weekday()
    this_monday = TODAY - timedelta(days=today_weekday)
    last_monday = this_monday - timedelta(days=7)
    next_monday = this_monday + timedelta(days=7)
    next_sunday = next_monday + timedelta(days=6)

    summary = {
        'last_week': {'start': last_monday, 'end': this_monday - timedelta(days=1)},
        'this_week': {'start': this_monday, 'end': next_monday - timedelta(days=1)},
        'next_week': {'start': next_monday, 'end': next_sunday},
    }

    for key, period in summary.items():
        if not opp_df.empty:
            mask = (opp_df['날짜'] >= period['start']) & (opp_df['날짜'] <= period['end'])
            week_df = opp_df[mask]
        else:
            week_df = pd.DataFrame()

        if not week_df.empty:
            period['total_opp'] = week_df[week_df['기회비용'] > 0]['기회비용'].sum()
            period['trigger_count'] = (week_df['BAR상승'] > 0).sum()
            period['affected_days'] = week_df[week_df['기회비용'] > 0]['날짜'].nunique()

            # TOP 3 누수일
            top_days = week_df[week_df['기회비용'] > 0].groupby(['날짜', '요일']).agg({
                '기회비용': 'sum', '시그널': lambda x: x.iloc[0]
            }).reset_index().sort_values('기회비용', ascending=False).head(3)
            period['top_days'] = top_days.to_dict('records')
        else:
            period['total_opp'] = 0
            period['trigger_count'] = 0
            period['affected_days'] = 0
            period['top_days'] = []

    return summary

# ============================================================
# 16. 📊 전년 동기 비교 & 누적 추적
# ============================================================
@st.cache_data(ttl=600)
def load_all_snapshots_history(days_back=400):
    """지난 N일간의 모든 스냅샷 (전년 비교용으로 400일)"""
    try:
        if not db_hotel: return []
        cutoff = (date.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        docs = db_hotel.collection("daily_snapshots").where("work_date", ">=", cutoff).stream()
        all_records = []
        for doc in docs:
            d_dict = doc.to_dict()
            work_date = d_dict.get('work_date')
            save_time = d_dict.get('save_time', '')
            if 'data' in d_dict:
                for row in d_dict['data']:
                    row['_work_date'] = work_date
                    row['_save_time'] = save_time
                    all_records.append(row)
        return all_records
    except:
        return []

def calculate_cumulative_metrics(snapshots_list, df_flight, df_comp,
                                  josun_threshold, flight_threshold,
                                  events, sensitivity, search_date_str=None):
    if not snapshots_list:
        return pd.DataFrame()
    snapshot_df = pd.DataFrame(snapshots_list)
    if snapshot_df.empty or '_work_date' not in snapshot_df.columns:
        return pd.DataFrame()
    results = []
    for work_date, group in snapshot_df.groupby('_work_date'):
        if '_save_time' in group.columns:
            latest_time = group['_save_time'].max()
            group = group[group['_save_time'] == latest_time]
        df = group.copy()
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.date
        try:
            opp = calculate_opportunity_cost(
                df, df_flight, df_comp,
                josun_threshold, flight_threshold,
                search_date_str, events, sensitivity
            )
            if not opp.empty:
                positive_opp = opp[opp['기회비용'] > 0]['기회비용'].sum()
                trigger_count = (opp['BAR상승'] > 0).sum()
                results.append({
                    'work_date': work_date,
                    'opportunity_cost': int(positive_opp),
                    'trigger_count': int(trigger_count),
                    'affected_days': opp[opp['기회비용'] > 0]['날짜'].nunique()
                })
        except:
            continue
    return pd.DataFrame(results)

def get_year_over_year_comparison(current_df, snapshots_list):
    """전년 동기 비교"""
    if current_df.empty:
        return pd.DataFrame()

    curr_dates = sorted(current_df['Date'].unique())
    if not curr_dates:
        return pd.DataFrame()

    yoy_data = []
    for d in curr_dates:
        try:
            last_year_date = d.replace(year=d.year - 1)
        except:
            continue

        # 현재 평균 가격
        curr_day_data = current_df[current_df['Date'] == d]
        curr_prices = []
        for rid in DYNAMIC_ROOMS:
            match = curr_day_data[curr_day_data['RoomID'] == rid]
            if not match.empty:
                _, _, price, _ = get_final_values(
                    rid, d, match.iloc[0]['Available'], match.iloc[0]['Total']
                )
                if price > 0:
                    curr_prices.append(price)

        curr_avg = sum(curr_prices) / len(curr_prices) if curr_prices else 0

        # 작년 같은 날
        last_year_prices = []
        if snapshots_list:
            for snap in snapshots_list:
                try:
                    snap_date = snap.get('Date')
                    if isinstance(snap_date, str):
                        snap_date = datetime.strptime(snap_date, '%Y-%m-%d').date()
                    if snap_date == last_year_date and snap.get('RoomID') in DYNAMIC_ROOMS:
                        _, _, ly_price, _ = get_final_values(
                            snap['RoomID'], last_year_date,
                            snap.get('Available', 0), snap.get('Total', 0)
                        )
                        if ly_price > 0:
                            last_year_prices.append(ly_price)
                except:
                    continue

        last_year_avg = sum(last_year_prices) / len(last_year_prices) if last_year_prices else None

        if curr_avg > 0:
            yoy_data.append({
                '날짜': d,
                '요일': WEEKDAYS_KR[d.weekday()],
                '올해_가격': int(curr_avg),
                '작년_같은날': last_year_date,
                '작년_가격': int(last_year_avg) if last_year_avg else None,
                '차이': int(curr_avg - last_year_avg) if last_year_avg else None,
                '증감률(%)': round((curr_avg - last_year_avg) / last_year_avg * 100, 1) if last_year_avg else None,
            })

    return pd.DataFrame(yoy_data)

# ============================================================
# 17. 🔄 정확도 검증 (A + B + C)
# ============================================================
def verify_simulator_accuracy(snapshots_list, df_flight, df_comp,
                               josun_threshold, flight_threshold,
                               events, sensitivity, search_date_str=None):
    if not snapshots_list:
        return {'verification_ready': False, 'message': '데이터가 아직 쌓이지 않았습니다.'}
    snapshot_df = pd.DataFrame(snapshots_list)
    if snapshot_df.empty or '_work_date' not in snapshot_df.columns:
        return {'verification_ready': False, 'message': '데이터 없음'}
    work_dates = sorted(snapshot_df['_work_date'].unique())
    if len(work_dates) < 2:
        return {'verification_ready': False,
                'message': f'최소 2일 이상의 스냅샷이 필요합니다. 현재 {len(work_dates)}일분.'}

    a_total, a_correct = 0, 0
    b_total, b_correct = 0, 0
    c_total, c_correct = 0, 0
    verification_details = []

    for i, work_date in enumerate(work_dates[:-1]):
        base_data = snapshot_df[snapshot_df['_work_date'] == work_date].copy()
        if '_save_time' in base_data.columns:
            latest = base_data['_save_time'].max()
            base_data = base_data[base_data['_save_time'] == latest]
        if 'Date' in base_data.columns:
            base_data['Date'] = pd.to_datetime(base_data['Date']).dt.date
        try:
            base_opp = calculate_opportunity_cost(
                base_data, df_flight, df_comp,
                josun_threshold, flight_threshold,
                search_date_str, events, sensitivity
            )
        except:
            continue
        if base_opp.empty:
            continue
        triggered = base_opp[base_opp['BAR상승'] > 0]
        if triggered.empty:
            continue
        later_dates = [wd for wd in work_dates if wd > work_date]
        if not later_dates:
            continue
        later_wd = later_dates[-1]
        later_data = snapshot_df[snapshot_df['_work_date'] == later_wd].copy()
        if '_save_time' in later_data.columns:
            latest = later_data['_save_time'].max()
            later_data = later_data[later_data['_save_time'] == latest]
        if 'Date' in later_data.columns:
            later_data['Date'] = pd.to_datetime(later_data['Date']).dt.date

        for _, trig_row in triggered.iterrows():
            check_date = trig_row['날짜']
            check_room = trig_row['객실타입']
            suggested_boost = trig_row['BAR상승']
            base_row = base_data[(base_data['Date'] == check_date) & (base_data['RoomID'] == check_room)]
            later_row = later_data[(later_data['Date'] == check_date) & (later_data['RoomID'] == check_room)]
            if base_row.empty or later_row.empty:
                continue
            try:
                base_avail_f = float(base_row.iloc[0]['Available']) if pd.notna(base_row.iloc[0]['Available']) else 0
                later_avail_f = float(later_row.iloc[0]['Available']) if pd.notna(later_row.iloc[0]['Available']) else 0
                later_total_f = float(later_row.iloc[0]['Total']) if pd.notna(later_row.iloc[0]['Total']) else 0
            except:
                continue
            pickup = base_avail_f - later_avail_f
            a_total += 1
            if pickup > 0:
                a_correct += 1
            if later_total_f > 0:
                final_occ = ((later_total_f - later_avail_f) / later_total_f) * 100
                b_total += 1
                if final_occ >= 50:
                    b_correct += 1
                c_total += 1
                if suggested_boost >= 1 and final_occ >= 50:
                    c_correct += 1
                elif suggested_boost >= 2 and final_occ >= 70:
                    c_correct += 1
            verification_details.append({
                '검증일': work_date, '대상일': check_date, '객실': check_room,
                '시뮬제안': f"+{suggested_boost}", '실제픽업': int(pickup),
                '최종점유율': f"{((later_total_f-later_avail_f)/later_total_f*100):.0f}%" if later_total_f > 0 else "-"
            })

    def pct(correct, total):
        return (correct / total * 100) if total > 0 else 0

    return {
        'verification_ready': True,
        'a_score': pct(a_correct, a_total), 'a_correct': a_correct, 'a_total': a_total,
        'b_score': pct(b_correct, b_total), 'b_correct': b_correct, 'b_total': b_total,
        'c_score': pct(c_correct, c_total), 'c_correct': c_correct, 'c_total': c_total,
        'details': verification_details[:50]
    }

# ============================================================
# 18. 파일 파서
# ============================================================
def robust_date_parser(d_val):
    if pd.isna(d_val): return None
    try:
        if isinstance(d_val, (int, float)):
            return (pd.to_datetime('1899-12-30') + pd.to_timedelta(d_val, 'D')).date()
        s = str(d_val).strip().replace('.', '-').replace('/', '-').replace(' ', '')
        match = re.search(r'(\d{1,2})-(\d{1,2})', s)
        if match:
            return date(2026, int(match.group(1)), int(match.group(2)))
    except: pass
    return None

def get_latest_snapshot(db_connection):
    """
    파라미터:
        db_connection: firestore.client 객체 (db_hotel을 넘겨받음)
    """
    if not db_connection:
        return pd.DataFrame(), None
    
    docs = db_connection.collection("daily_snapshots").order_by("save_time", direction=firestore.Query.DESCENDING).limit(1).stream()
    for doc in docs:
        d_dict = doc.to_dict()
        df = pd.DataFrame(d_dict['data'])
        if not df.empty and 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.date
        return df, d_dict.get('work_date', '알수없음')
    return pd.DataFrame(), None
# ============================================================
# 18-A. UI 필터용 헬퍼 함수 (신규 추가)
# ============================================================
def date_filter_toggle(key_prefix, total_dates, default_show_past=False):
    past_count = sum(1 for d in total_dates if d < TODAY)
    future_count = len(total_dates) - past_count
    
    if past_count == 0:
        return total_dates
    
    show_past = st.checkbox(
        f"📜 과거 {past_count}일 포함 (현재 미래 {future_count}일만 표시)",
        value=default_show_past,
        key=f"show_past_{key_prefix}"
    )
    
    if show_past:
        return total_dates
    else:
        return [d for d in total_dates if d >= TODAY]

def filter_df_by_dates(df, visible_dates):
    if df.empty: return df
    return df[df['Date'].isin(visible_dates)].copy()

# ============================================================
# 19. 메모 입력 위젯
# ============================================================
def render_note_input(key_id, label="메모", notes=None, show_tag=True, widget_prefix=""):
    if notes is None: notes = {}
    existing = notes.get(key_id, {})
    existing_content = existing.get('content', '')
    existing_tag = existing.get('tag', '일반')
    # widget_prefix를 붙여서 탭마다 key가 중복되지 않게
    unique_key = f"{widget_prefix}_{key_id}" if widget_prefix else key_id
    col1, col2 = st.columns([4, 1]) if show_tag else (st.container(), None)
    with col1:
        new_content = st.text_area(label, value=existing_content, key=f"note_input_{unique_key}",
                                    height=80, placeholder="예: 총지배인 지시로 유지 / 단체예약 / 다시 확인")
    if show_tag and col2:
        with col2:
            tag_options = ['일반', '의사결정', '경고', '대기']
            new_tag = st.selectbox("태그", tag_options,
                index=tag_options.index(existing_tag) if existing_tag in tag_options else 0,
                key=f"note_tag_{unique_key}")
    else:
        new_tag = existing_tag
    if st.button("💾 저장", key=f"note_save_{unique_key}"):
        if save_note(key_id, new_content, new_tag):
            st.success("저장되었습니다!")
            st.rerun()

# ============================================================
# 20. 기본 테이블 렌더러 (통합 관제 모드 추가)
# ============================================================
def get_bar_price(room_id, bar):
    if bar == "BAR0":
        if room_id in DYNAMIC_ROOMS: return PRICE_TABLE.get(room_id, {}).get("BAR0", 0)
        else: return FIXED_BAR0_TABLE.get(room_id, 0)
    if room_id in DYNAMIC_ROOMS: return PRICE_TABLE.get(room_id, {}).get(bar, 0)
    else: return FIXED_PRICE_TABLE.get(room_id, {}).get(bar, 0)

def render_master_table(current_df, prev_df, title="", mode="기준", applied_rates=None):
    if current_df.empty:
        return "<div style='padding:20px;'>데이터를 업로드하세요.</div>"
    dates = sorted(current_df['Date'].unique())
    row_padding = "8px"; header_padding = "5px"; font_size = "11px"
    
    # 타이틀 테마 색상
    if mode == "통합관제":
        bg_title = "#FFF8E1"; border_title = "#FF8F00"
    else:
        bg_title = "#f0f2f6"; border_title = "#000"
        
    html = f"<div style='margin-top:40px; margin-bottom:10px; font-weight:bold; font-size:18px; padding:10px; background:{bg_title}; border-left:10px solid {border_title};'>{title}</div>"
    html += "<div style='overflow-x: auto; white-space: nowrap; border: 1px solid #ddd;'>"
    html += f"<table style='width:100%; border-collapse:collapse; font-size:{font_size}; min-width:1000px;'>"
    html += "<thead><tr style='background:#f9f9f9;'>"
    html += f"<th rowspan='2' style='border:1px solid #ddd; width:120px; position:sticky; left:0; background:#f9f9f9; z-index:2; padding:{header_padding};'>객실</th>"
    for d in dates:
        is_past = d < TODAY
        past_style = "background:#EEEEEE; color:#999;" if is_past else ""
        html += f"<th style='border:1px solid #ddd; padding:{header_padding}; {past_style}'>{d.strftime('%m-%d')}</th>"
    html += "</tr><tr style='background:#f9f9f9;'>"
    for d in dates:
        wd = WEEKDAYS_KR[d.weekday()]
        is_past = d < TODAY
        if is_past:
            color = "#999"
        else:
            color = "red" if wd == '일' else ("blue" if wd == '토' else "black")
        past_style = "background:#EEEEEE;" if is_past else ""
        html += f"<th style='border:1px solid #ddd; padding:{header_padding}; color:{color}; {past_style}'>{wd}</th>"
    html += "</tr></thead><tbody>"
    
    for rid in ALL_ROOMS:
        label = f"<b>{rid}</b>" if rid in ["HDF", "PPV"] else rid
        border_thick = "border-bottom:3.4px solid #000;" if rid in ["HDF", "PPV"] else ""
        html += f"<tr style='{border_thick}'><td style='border:1px solid #ddd; padding:{row_padding}; background:#fff; border-right:4px solid #000; position:sticky; left:0; z-index:1;'>{label}</td>"
        
        for d in dates:
            is_past = d < TODAY
            date_str = d.strftime('%Y-%m-%d')
            curr_match = current_df[(current_df['RoomID'] == rid) & (current_df['Date'] == d)]
            if curr_match.empty:
                html += f"<td style='border:1px solid #ddd; padding:{row_padding}; text-align:center;'>-</td>"
                continue
            
            avail = curr_match.iloc[0]['Available']; total = curr_match.iloc[0]['Total']
            occ = ((total - avail) / total * 100) if total > 0 else 0
            
            # [핵심] 통합관제를 위해 순수 시스템 권장가를 따로 도출
            type_code, season, is_weekend = get_season_details(d)
            pure_sys_bar = determine_bar(season, is_weekend, occ) if rid in DYNAMIC_ROOMS else type_code
            
            # get_final_values 호출 (여기에 이미 오버라이드 값이 섞여 나옴)
            _, bar, base_price, _ = get_final_values(rid, d, avail, total)
            
            prev_bar, prev_avail = None, None
            if not prev_df.empty:
                prev_m = prev_df[(prev_df['RoomID'] == rid) & (prev_df['Date'] == d)]
                if not prev_m.empty:
                    prev_avail = prev_m.iloc[0]['Available']
                    _, prev_bar, _, _ = get_final_values(rid, d, prev_avail, prev_m.iloc[0]['Total'])
                    
            style = f"border:1px solid #ddd; padding:{row_padding}; text-align:center; background-color:white;"
            
            if mode == "기준":
                bg = BAR_GRADIENT_COLORS.get(bar, "#FFFFFF") if rid in DYNAMIC_ROOMS or bar == "BAR0" else "#F1F1F1"
                style += f"background-color: {bg};"
                if is_past:
                    content = f"<b>{bar}</b><br>{base_price:,}<br>{occ:.0f}%<br><span style='font-size:8px; color:#666;'>📜마감</span>"
                else:
                    content = f"<b>{bar}</b><br>{base_price:,}<br>{occ:.0f}%"
                    
            elif mode == "통합관제":
                applied_bar = applied_rates.get(date_str, {}).get('rooms', {}).get(rid) if applied_rates else None
                is_applied = applied_bar is not None
                
                # 수동 개입이 없다면, 현재 CMS 요금은 어제 요금(prev_bar)에 머물러 있다고 전제함
                current_cms_bar = prev_bar if prev_bar else pure_sys_bar
                
                if is_past:
                    final_bar = applied_bar if is_applied else current_cms_bar
                    final_price = get_bar_price(rid, final_bar)
                    bg = BAR_GRADIENT_COLORS.get(final_bar, "#FFFFFF") if rid in DYNAMIC_ROOMS or final_bar == "BAR0" else "#F1F1F1"
                    style += f"background-color: {bg}; opacity: 0.6;"
                    content = f"<b>{final_bar}</b><br>{final_price:,}<br><span style='font-size:8px; color:#444;'>📜마감</span>"
                else:
                    if is_applied:
                        final_bar = applied_bar
                        final_price = get_bar_price(rid, final_bar)
                        bg = BAR_GRADIENT_COLORS.get(final_bar, "#FFFFFF") if rid in DYNAMIC_ROOMS or final_bar == "BAR0" else "#F1F1F1"
                        
                        if applied_bar == pure_sys_bar:
                            # 1. 수동으로 확정했는데, 시스템 권장가랑 똑같음
                            style += f"background-color: {bg}; border: 2.5px solid #2E7D32;"
                            content = f"<div style='font-size:9px; color:#2E7D32; font-weight:bold; margin-bottom:2px;'>✅ 전략 일치</div>"
                            content += f"<b>{final_bar}</b><br>{final_price:,}<br>{occ:.0f}%"
                        else:
                            # 2. 수동으로 시스템 권장가와 다르게 덮어씌움 (비싸게든 싸게든)
                            style += f"background-color: {bg}; border: 2.5px dashed #D32F2F;"
                            content = f"<div style='font-size:9px; color:#D32F2F; font-weight:bold; margin-bottom:2px;'>⭐ 수동 개입</div>"
                            content += f"<b>{final_bar}</b><br>{final_price:,}<br>{occ:.0f}%<br>"
                            content += f"<div style='font-size:10px; color:#333; margin-top:3px; background:rgba(255,255,255,0.7); padding:2px; border-radius:3px;'>💡권장: <del>{pure_sys_bar}</del></div>"
                    else:
                        # 수동 개입 안 한 경우 (어제 요금 그대로 유지 중)
                        final_bar = current_cms_bar
                        final_price = get_bar_price(rid, final_bar)
                        bg = BAR_GRADIENT_COLORS.get(final_bar, "#FFFFFF") if rid in DYNAMIC_ROOMS or final_bar == "BAR0" else "#F1F1F1"
                        
                        if pure_sys_bar != current_cms_bar:
                            # 3. ⚠️ 핵심 추가: 시스템은 바꾸라고 했는데 안 바꾼 날 (의도적 홀딩 or 누수)
                            style += f"background-color: {bg}; border: 2.5px solid #FF8F00;"
                            content = f"<div style='font-size:9px; color:#FF8F00; font-weight:bold; margin-bottom:2px;'>⚠️ 미적용 (수익포기)</div>"
                            content += f"<b>{final_bar}</b><br>{final_price:,}<br>{occ:.0f}%<br>"
                            content += f"<div style='font-size:10px; color:#333; margin-top:3px; background:rgba(255,255,255,0.9); padding:2px; border-radius:3px;'>💡권장: <b style='color:#D32F2F;'>{pure_sys_bar}</b></div>"
                        else:
                            # 4. 시스템도 어제 요금 그대로 유지하라고 함
                            style += f"background-color: {bg}; opacity: 0.9;"
                            content = f"<div style='font-size:9px; color:#757575; font-weight:bold; margin-bottom:2px;'>🤖 변동 없음</div>"
                            content += f"<b>{final_bar}</b><br>{final_price:,}<br>{occ:.0f}%"
                        
            elif mode == "변화":
                curr_av = float(avail) if pd.notna(avail) else 0.0
                prev_av = float(prev_avail) if (prev_avail is not None and pd.notna(prev_avail)) else 0.0
                pickup = (prev_av - curr_av) if prev_avail is not None else 0
                if is_past:
                    style += "background-color: #F5F5F5; color:#888;"
                    content = "-" if pickup == 0 else (f"+{pickup:.0f}" if pickup > 0 else f"{pickup:.0f}")
                else:
                    bg = BAR_LIGHT_COLORS.get(bar, "#FFFFFF") if rid in DYNAMIC_ROOMS or bar == "BAR0" else "#FFFFFF"
                    style += f"background-color: {bg};"
                    if pickup > 0:
                        style += "color:red; font-weight:bold; border: 1.5px solid red;"
                        content = f"+{pickup:.0f}"
                    elif pickup < 0:
                        style += "color:blue; font-weight:bold;"
                        content = f"{pickup:.0f}"
                    else: content = "-"
                    
            elif mode == "판도변화":
                curr_b = str(bar).strip() if bar else ""
                prev_b = str(prev_bar).strip() if prev_bar else ""
                if is_past:
                    style += "background-color: #F5F5F5; color:#888;"
                    content = bar
                elif prev_bar is not None and prev_b != curr_b:
                    bg = BAR_GRADIENT_COLORS.get(bar, "#7000FF")
                    style += f"background-color: {bg}; color: white; font-weight: bold; border: 2.5px solid #000;"
                    content = f"▲ {bar}"
                else: content = bar
                
            html += f"<td style='{style}'>{content}</td>"
        html += "</tr>"
    html += "</tbody></table></div>"
    return html

# ============================================================
# 21. 시뮬레이터 테이블 (과거 회색 처리)
# ============================================================
def render_sim_comparison_table(current_df, df_flight, df_comp,
                                 josun_threshold, flight_threshold,
                                 search_date_str=None, events=None, sensitivity=None):
    if current_df.empty:
        return "<div style='padding:20px;'>데이터를 업로드하세요.</div>"
    dates = sorted(current_df['Date'].unique())
    html = "<div style='overflow-x:auto; white-space:nowrap; border:1px solid #ddd;'>"
    html += "<table style='width:100%; border-collapse:collapse; font-size:11px; min-width:1000px;'>"
    html += "<thead><tr style='background:#1a1a2e; color:white;'>"
    html += "<th rowspan='3' style='border:1px solid #444; width:70px; padding:5px; position:sticky; left:0; background:#1a1a2e; z-index:2;'>객실</th>"
    for d in dates:
        is_past = d < TODAY
        past_txt = " 📜" if is_past else ""
        html += f"<th colspan='2' style='border:1px solid #444; padding:4px; text-align:center;'>{d.strftime('%m-%d')}{past_txt}</th>"
    html += "</tr><tr style='background:#1a1a2e; color:white;'>"
    for d in dates:
        wd = WEEKDAYS_KR[d.weekday()]
        is_past = d < TODAY
        if is_past:
            color = "#888"
        else:
            color = "#FF6B6B" if wd == '일' else ("#74B9FF" if wd == '토' else "white")
        html += f"<th colspan='2' style='border:1px solid #444; padding:2px; color:{color};'>{wd}</th>"
    html += "</tr><tr style='background:#2d2d44; color:white;'>"
    for d in dates:
        html += "<th style='border:1px solid #444; padding:3px; color:#FF8A80;'>실제</th>"
        html += "<th style='border:1px solid #444; padding:3px; color:#80D8FF;'>시뮬</th>"
    html += "</tr></thead><tbody>"

    html += "<tr style='background:#f5f5f5;'>"
    html += "<td style='border:1px solid #ddd; padding:4px; font-weight:bold; position:sticky; left:0; background:#f5f5f5; z-index:1; font-size:10px;'>시장+이벤트</td>"
    for d in dates:
        josun_p, parnas_p, flight_p = get_market_price_for_date(d, df_flight, df_comp, search_date_str)
        j_color = "#C62828" if (josun_p and josun_p >= josun_threshold) else "#555"
        f_color = "#1565C0" if (flight_p and flight_p >= flight_threshold) else "#555"
        j_txt = f"<span style='color:{j_color}; font-weight:bold;'>조선 {int(josun_p):,}</span>" if josun_p else "<span style='color:#aaa;'>조선 -</span>"
        f_txt = f"<span style='color:{f_color}; font-weight:bold;'>항공 {int(flight_p):,}</span>" if flight_p else "<span style='color:#aaa;'>항공 -</span>"
        ev_boost, ev_names = get_event_boost_for_date(d, events or [])
        ev_txt = f"<span style='color:#FF6F00; font-weight:bold;'>🎉 {ev_names[0][:6]}</span>" if ev_names else ""
        html += f"<td colspan='2' style='border:1px solid #ddd; padding:3px; text-align:center; font-size:10px;'>{j_txt}<br>{f_txt}<br>{ev_txt}</td>"
    html += "</tr>"

    for rid in DYNAMIC_ROOMS:
        sens_val = (sensitivity or {}).get(rid, 1.0)
        sens_label = f" <span style='font-size:9px; color:#888;'>×{sens_val}</span>" if sens_val != 1.0 else ""
        html += "<tr>"
        html += f"<td style='border:1px solid #ddd; padding:6px; font-weight:bold; background:#fff; position:sticky; left:0; z-index:1;'>{rid}{sens_label}</td>"
        for d in dates:
            is_past = d < TODAY
            curr_match = current_df[(current_df['RoomID'] == rid) & (current_df['Date'] == d)]
            if curr_match.empty:
                html += "<td style='border:1px solid #ddd; text-align:center;'>-</td>"
                html += "<td style='border:1px solid #ddd; text-align:center;'>-</td>"
                continue
            avail = curr_match.iloc[0]['Available']; total = curr_match.iloc[0]['Total']
            occ, real_bar, real_price, _ = get_final_values(rid, d, avail, total)
            josun_p, parnas_p, flight_p = get_market_price_for_date(d, df_flight, df_comp, search_date_str)
            prev_week_date = d - timedelta(days=7)
            josun_prev, _, _ = get_market_price_for_date(prev_week_date, df_flight, df_comp, search_date_str)
            _, sim_bar, sim_price, boost, signal_str = get_sim_bar(
                rid, d, avail, total, josun_p, flight_p, parnas_p,
                josun_threshold, flight_threshold,
                josun_prev_price=josun_prev, events=events, sensitivity=sensitivity
            )

            # 색은 원래대로 유지, 과거는 작게 표시만
            real_bg = BAR_GRADIENT_COLORS.get(real_bar, "#fff")
            real_style = f"border:1px solid #ddd; padding:5px; text-align:center; background:{real_bg}; font-size:11px;"
            sim_bg = SIM_BAR_COLORS.get(sim_bar, "#fff")
            diff = sim_price - real_price
            sim_style = f"border:1px solid #ddd; padding:5px; text-align:center; background:{sim_bg}; color:white; font-size:11px;"

            if boost > 0:
                sim_style += "border:2px solid #FFD700;"
                diff_txt = f"<span style='color:#FFD700; font-size:9px;'>+{boost}단계 +{diff:,}</span>"
            else:
                diff_txt = ""

            if is_past:
                # 과거: 색은 유지, 📜 아이콘만 추가
                real_content = f"<b>{real_bar}</b><br><span style='font-size:10px;'>{real_price:,}</span><br><span style='font-size:8px; color:#555;'>📜</span>"
                sim_content = f"<b>{sim_bar}</b><br><span style='font-size:10px;'>{sim_price:,}</span><br>{diff_txt if diff_txt else '<span style=\"font-size:8px; color:#FFD700;\">📜</span>'}"
            else:
                real_content = f"<b>{real_bar}</b><br><span style='font-size:10px;'>{real_price:,}</span>"
                sim_content = f"<b>{sim_bar}</b><br><span style='font-size:10px;'>{sim_price:,}</span><br>{diff_txt}"

            html += f"<td style='{real_style}'>{real_content}</td>"
            html += f"<td style='{sim_style}'>{sim_content}</td>"
        html += "</tr>"
    html += "</tbody></table></div>"
    return html

# ============================================================
# 22. 📄 PDF 생성 (한글 폰트 지원!)
# ============================================================
def _fig_to_image_bytes(fig, width=800, height=400):
    """Plotly 차트를 PNG 바이트로 변환"""
    try:
        return fig.to_image(format="png", width=width, height=height, scale=2)
    except Exception:
        return None

def _safe_multicell(pdf, w, h, text, max_chars_per_line=None):
    """안전한 multi_cell - 너비 부족 에러 방지"""
    try:
        pdf.multi_cell(w, h, text)
    except Exception:
        # 공간 부족 시 폰트 줄이고 재시도
        current_size = pdf.font_size_pt
        try:
            pdf.set_font_size(max(7, current_size - 2))
            pdf.multi_cell(w, h, text)
            pdf.set_font_size(current_size)
        except Exception:
            # 그래도 실패하면 텍스트 자르기
            pdf.set_font_size(current_size)
            short_text = text[:100] + "..." if len(text) > 100 else text
            try:
                pdf.multi_cell(w, h, short_text)
            except:
                pass

def generate_pdf_report(opp_df, period_label, josun_threshold, flight_threshold,
                         start_date, end_date, curr_df=None, df_flight=None, df_comp=None,
                         events=None, sensitivity=None, active_search_date=None,
                         snapshots_list=None):
    """고급 컨설팅 수준 PDF 보고서 (안전 버전)"""
    pdf = FPDF()
    pdf.set_margins(20, 20, 20)
    pdf.set_auto_page_break(auto=True, margin=25)

    # 한글 폰트 로드
    font_path = "NanumGothic.ttf"
    font_loaded = False
    if os.path.exists(font_path):
        try:
            pdf.add_font('NanumGothic', '', font_path, uni=True)
            pdf.add_font('NanumGothic', 'B', font_path, uni=True)
            font_loaded = True
        except:
            font_loaded = False
    KFONT = 'NanumGothic' if font_loaded else 'helvetica'

    def T(ko, en):
        return ko if font_loaded else en

    # ============ 페이지 1: 표지 ============
    pdf.add_page()
    pdf.set_fill_color(26, 42, 68)
    pdf.rect(0, 0, 210, 297, 'F')
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(0, 95, 210, 4, 'F')
    pdf.set_text_color(255, 255, 255)
    pdf.set_font(KFONT, 'B', 28)
    pdf.set_xy(20, 115)
    pdf.cell(0, 15, "AMBER PURE HILL", ln=True)
    pdf.set_font(KFONT, 'B', 18)
    pdf.cell(0, 12, T("수익 최적화 전략 보고서", "Revenue Optimization Report"), ln=True)
    pdf.set_font(KFONT, '', 12)
    pdf.ln(5)
    pdf.set_text_color(200, 200, 200)
    pdf.cell(0, 8, "Strategic Revenue Management", ln=True)
    pdf.ln(40)
    pdf.set_font(KFONT, '', 11)
    pdf.cell(0, 8, T(f"기간: {period_label}", f"Period: {period_label}"), ln=True, align='R')
    pdf.cell(0, 8, T(f"보고일: {date.today().strftime('%Y-%m-%d')}",
                      f"Report Date: {date.today().strftime('%Y-%m-%d')}"), ln=True, align='R')
    pdf.cell(0, 8, T(f"분석 범위: {start_date} ~ {end_date}",
                      f"Range: {start_date} ~ {end_date}"), ln=True, align='R')
    pdf.set_y(270)
    pdf.set_font(KFONT, '', 9)
    pdf.set_text_color(166, 138, 86)
    pdf.cell(0, 10, T("대외비 - 전략 수익 분석 보고서", "CONFIDENTIAL"), 0, 0, 'C')

    # ============ 페이지 2: 핵심 요약 ============
    pdf.add_page()
    pdf.set_text_color(26, 42, 68)
    pdf.set_font(KFONT, 'B', 20)
    pdf.cell(0, 12, T("01. 핵심 요약", "01. EXECUTIVE SUMMARY"), ln=True)
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(20, 32, 30, 2, 'F')
    pdf.ln(10)

    if not opp_df.empty:
        positive_opp = opp_df[opp_df['기회비용'] > 0]['기회비용'].sum()
        boosted_count = (opp_df['BAR상승'] > 0).sum()
        avg_diff = opp_df[opp_df['기회비용'] > 0]['단가차이'].mean() if len(opp_df[opp_df['기회비용'] > 0]) > 0 else 0
        days_affected = opp_df[opp_df['기회비용'] > 0]['날짜'].nunique()

        pdf.set_fill_color(248, 245, 240)
        pdf.rect(20, pdf.get_y(), 170, 50, 'F')
        pdf.set_xy(20, pdf.get_y() + 5)
        pdf.set_font(KFONT, '', 11)
        pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 8, T("추정 추가 매출 (기회비용)", "Estimated Additional Revenue"), ln=True, align='C')
        pdf.set_font(KFONT, 'B', 28)
        pdf.set_text_color(166, 138, 86)
        pdf.cell(0, 18, f"KRW {int(positive_opp):,}", ln=True, align='C')
        pdf.set_font(KFONT, '', 10)
        pdf.set_text_color(120, 120, 120)
        pdf.cell(0, 6, T("현재 가격 정책에서 포착되지 않은 수익 기회",
                          "Revenue opportunity not captured"), ln=True, align='C')
        pdf.ln(15)

        # KPI 테이블
        pdf.set_text_color(0, 0, 0)
        pdf.set_font(KFONT, 'B', 11)
        pdf.set_fill_color(26, 42, 68)
        pdf.set_text_color(255, 255, 255)
        pdf.cell(95, 9, T("지표", "Metric"), 1, 0, 'C', True)
        pdf.cell(75, 9, T("값", "Value"), 1, 1, 'C', True)
        pdf.set_font(KFONT, '', 10)
        pdf.set_text_color(0, 0, 0)
        rows_data = [
            (T("시그널 발동 건수", "Signal Triggers"), f"{boosted_count}"),
            (T("영향 받은 날짜", "Days Affected"), f"{days_affected}"),
            (T("건당 평균 단가 차이", "Avg Price Gap"), f"KRW {int(avg_diff):,}"),
            (T("분석 기간", "Period"), period_label),
            (T("조선 임계가", "Josun Threshold"), f"KRW {josun_threshold:,}"),
            (T("항공 임계가", "Flight Threshold"), f"KRW {flight_threshold:,}"),
        ]
        fill_toggle = False
        for label, val in rows_data:
            fill = fill_toggle
            if fill: pdf.set_fill_color(248, 248, 248)
            pdf.cell(95, 8, f"  {label}", 1, 0, 'L', fill)
            pdf.cell(75, 8, str(val), 1, 1, 'C', fill)
            fill_toggle = not fill_toggle

        pdf.ln(8)

        # 핵심 메시지
        pdf.set_fill_color(255, 243, 224)
        y_box = pdf.get_y()
        pdf.rect(20, y_box, 170, 35, 'F')
        pdf.set_xy(25, y_box + 5)
        pdf.set_font(KFONT, 'B', 11)
        pdf.set_text_color(166, 138, 86)
        pdf.cell(0, 7, T("[ 핵심 메시지 ]", "[ Key Message ]"), ln=True)
        pdf.set_xy(25, pdf.get_y() + 1)
        pdf.set_font(KFONT, '', 9)
        pdf.set_text_color(60, 60, 60)
        key_msg = T(
            f"현재 점유율 중심 정책 하에서 {days_affected}일간 {boosted_count}건의 시장 시그널이 포착되었으나, ADR 상향이 이루어지지 않아 약 KRW {int(positive_opp):,}의 추가 수익 기회를 놓쳤습니다.",
            f"Under current policy, {boosted_count} signals detected but KRW {int(positive_opp):,} opportunity missed."
        )
        _safe_multicell(pdf, 160, 5, key_msg)

    # ============ 페이지 3: 차트 ============
    pdf.add_page()
    pdf.set_text_color(26, 42, 68)
    pdf.set_font(KFONT, 'B', 20)
    pdf.cell(0, 12, T("02. 데이터 시각화", "02. DATA VISUALIZATION"), ln=True)
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(20, 32, 30, 2, 'F')
    pdf.ln(8)

    if not opp_df.empty:
        pdf.set_font(KFONT, 'B', 13)
        pdf.set_text_color(166, 138, 86)
        pdf.cell(0, 8, T("날짜별 기회비용 분포", "Daily Opportunity Cost"), ln=True)
        pdf.ln(2)

        daily_opp = opp_df.groupby(['날짜', '요일'])['기회비용'].sum().reset_index()
        daily_opp['라벨'] = daily_opp['날짜'].apply(lambda x: x.strftime('%m-%d'))

        try:
            fig1 = px.bar(daily_opp, x='라벨', y='기회비용',
                          color='기회비용', color_continuous_scale=['#E8F5E9', '#FF5252'])
            fig1.update_layout(template="plotly_white", height=350, width=700,
                                showlegend=False, margin=dict(l=50, r=20, t=30, b=50),
                                xaxis_title="", yaxis_title="KRW", font=dict(size=11))
            img_bytes = _fig_to_image_bytes(fig1, width=700, height=350)
            if img_bytes:
                import tempfile
                with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                    tmp.write(img_bytes)
                    tmp_path = tmp.name
                pdf.image(tmp_path, x=20, w=170)
                try:
                    os.unlink(tmp_path)
                except: pass
        except Exception:
            pass

        pdf.ln(5)
        pdf.set_font(KFONT, 'B', 13)
        pdf.set_text_color(166, 138, 86)
        pdf.cell(0, 8, T("객실 타입별 기회비용", "Opportunity Cost by Room Type"), ln=True)
        pdf.ln(2)

        room_opp = opp_df.groupby('객실타입')['기회비용'].sum().reset_index().sort_values('기회비용', ascending=True)
        try:
            fig2 = px.bar(room_opp, x='기회비용', y='객실타입', orientation='h',
                          color='기회비용', color_continuous_scale=['#E8F5E9', '#FF5252'])
            fig2.update_layout(template="plotly_white", height=280, width=700,
                                showlegend=False, margin=dict(l=50, r=20, t=20, b=40),
                                xaxis_title="KRW", yaxis_title="", font=dict(size=11))
            img_bytes = _fig_to_image_bytes(fig2, width=700, height=280)
            if img_bytes:
                import tempfile
                with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                    tmp.write(img_bytes)
                    tmp_path = tmp.name
                pdf.image(tmp_path, x=20, w=170)
                try:
                    os.unlink(tmp_path)
                except: pass
        except Exception:
            pass

# ============ 페이지 4: 대표 사례 ============
    pdf.add_page()
    pdf.set_text_color(26, 42, 68)
    pdf.set_font(KFONT, 'B', 20)
    pdf.cell(0, 12, T("03. 대표 사례 분석", "03. REPRESENTATIVE CASES"), ln=True)
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(20, 32, 30, 2, 'F')
    pdf.ln(8)

    if not opp_df.empty:
        pdf.set_font(KFONT, '', 10)
        pdf.set_text_color(80, 80, 80)
        _safe_multicell(pdf, 0, 6, T(
            "가장 큰 기회비용이 발생한 TOP 3 날짜를 상세 분석합니다.",
            "Top 3 dates with highest opportunity cost analyzed in detail."
        ))
        pdf.ln(5)

        top3 = opp_df[opp_df['기회비용'] > 0].nlargest(3, '기회비용')

        for idx, (_, case) in enumerate(top3.iterrows()):
            case_date = case['날짜']
            case_room = case['객실타입']
            sold = int(case['판매객실수'])
            real_bar = case['실제BAR']
            real_p = int(case['실제단가'])
            sim_bar = case['시뮬BAR']
            sim_p = int(case['시뮬단가'])
            opp = int(case['기회비용'])
            signal = str(case['시그널'])[:50]

            # 케이스 제목
            pdf.set_fill_color(26, 42, 68)
            pdf.rect(20, pdf.get_y(), 170, 10, 'F')
            pdf.set_xy(22, pdf.get_y() + 2)
            pdf.set_font(KFONT, 'B', 11)
            pdf.set_text_color(255, 255, 255)
            title_txt = T(
                f"  CASE {idx+1}. {case_date.strftime('%Y-%m-%d')} ({WEEKDAYS_KR[case_date.weekday()]}) | {case_room} | 누수 KRW {opp:,}",
                f"  CASE {idx+1}. {case_date.strftime('%Y-%m-%d')} | {case_room} | Loss KRW {opp:,}"
            )
            pdf.cell(0, 6, title_txt, ln=True)
            pdf.ln(5)

            pdf.set_text_color(0, 0, 0)

            # 시장 상황
            pdf.set_font(KFONT, 'B', 10)
            pdf.set_text_color(166, 138, 86)
            pdf.cell(0, 6, T("> 시장 상황", "> Market Situation"), ln=True)
            pdf.set_font(KFONT, '', 9)
            pdf.set_text_color(60, 60, 60)
            _safe_multicell(pdf, 0, 5, T(
                f"시뮬레이터 포착 시그널: {signal}. 외부 시장이 명확한 상승 신호를 보였습니다.",
                f"Signals: {signal}. Market showed upward momentum."
            ))
            pdf.ln(2)

            # 우리의 판단
            pdf.set_font(KFONT, 'B', 10)
            pdf.set_text_color(166, 138, 86)
            pdf.cell(0, 6, T("> 우리의 판단", "> Our Decision"), ln=True)
            pdf.set_font(KFONT, '', 9)
            pdf.set_text_color(60, 60, 60)
            _safe_multicell(pdf, 0, 5, T(
                f"{case_room} 객실은 {real_bar} (KRW {real_p:,})으로 판매. 점유율 기반 정책 결과로, 외부 시그널 미반영.",
                f"{case_room} sold at {real_bar} (KRW {real_p:,}) - occupancy-only policy."
            ))
            pdf.ln(2)

            # 시뮬 제안
            pdf.set_font(KFONT, 'B', 10)
            pdf.set_text_color(166, 138, 86)
            pdf.cell(0, 6, T("> 시뮬레이터 제안", "> Simulator Recommendation"), ln=True)
            pdf.set_font(KFONT, '', 9)
            pdf.set_text_color(60, 60, 60)
            _safe_multicell(pdf, 0, 5, T(
                f"시뮬은 {sim_bar} (KRW {sim_p:,}) 제안. 단가 차이 KRW {sim_p - real_p:,}, {sold}실 판매 기준 총 KRW {opp:,} 추가 수익 가능.",
                f"Sim recommended {sim_bar} (KRW {sim_p:,}). Gap x {sold} rooms = KRW {opp:,}."
            ))
            pdf.ln(2)

            # 교훈
            pdf.set_font(KFONT, 'B', 10)
            pdf.set_text_color(166, 138, 86)
            pdf.cell(0, 6, T("> 교훈", "> Lesson"), ln=True)
            pdf.set_font(KFONT, '', 9)
            pdf.set_text_color(60, 60, 60)
            _safe_multicell(pdf, 0, 5, T(
                "점유율 충족 + 시장 강세 상황에서 ADR 상향 기회가 있었습니다. 브랜드 포지셔닝 강화와 매출 확보가 동시에 가능한 전형 사례.",
                "When occupancy is strong and market is bullish, ADR upside exists."
            ))
            pdf.ln(6)

    # ============ 페이지 5: 경쟁사 벤치마크 ============
    pdf.add_page()
    pdf.set_text_color(26, 42, 68)
    pdf.set_font(KFONT, 'B', 20)
    pdf.cell(0, 12, T("04. 경쟁사 벤치마크", "04. COMPETITOR BENCHMARK"), ln=True)
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(20, 32, 30, 2, 'F')
    pdf.ln(8)

    pdf.set_font(KFONT, '', 10)
    pdf.set_text_color(80, 80, 80)
    _safe_multicell(pdf, 0, 6, T(
        "제주 5성급 시장의 주요 경쟁사와 엠버퓨어힐의 가격 포지셔닝을 비교합니다.",
        "Price positioning comparison with major Jeju competitors."
    ))
    pdf.ln(5)

    if curr_df is not None and df_comp is not None and not df_comp.empty:
        dates_list = sorted(curr_df['Date'].unique())
        josun_prices, parnas_prices = [], []

        for d in dates_list:
            try:
                j, p, _ = get_market_price_for_date(d, df_flight, df_comp, active_search_date)
                if j: josun_prices.append(j)
                if p: parnas_prices.append(p)
            except:
                continue

        our_avg_dict = get_our_avg_price_for_dates(curr_df, dates_list)
        amber_prices = [v for v in our_avg_dict.values() if v]

        avg_josun = sum(josun_prices) / len(josun_prices) if josun_prices else 0
        avg_parnas = sum(parnas_prices) / len(parnas_prices) if parnas_prices else 0
        avg_amber = sum(amber_prices) / len(amber_prices) if amber_prices else 0

        # 테이블
        pdf.set_font(KFONT, 'B', 11)
        pdf.set_fill_color(26, 42, 68)
        pdf.set_text_color(255, 255, 255)
        pdf.cell(55, 9, T("호텔", "Hotel"), 1, 0, 'C', True)
        pdf.cell(45, 9, T("평균가", "Avg Price"), 1, 0, 'C', True)
        pdf.cell(35, 9, T("포지셔닝", "Position"), 1, 0, 'C', True)
        pdf.cell(35, 9, T("조선 대비", "vs Josun"), 1, 1, 'C', True)

        pdf.set_font(KFONT, '', 10)
        pdf.set_text_color(0, 0, 0)

        comp_data = [
            (T("그랜드 조선 제주", "Grand Josun"), avg_josun, T("프리미엄", "Premium"), 0),
            (T("파르나스 제주", "Parnas Jeju"), avg_parnas, T("프리미엄", "Premium"),
             avg_parnas - avg_josun if avg_josun else 0),
            (T("엠버퓨어힐", "Amber Pure Hill"), avg_amber, T("하이엔드", "High-end"),
             avg_amber - avg_josun if avg_josun else 0),
        ]
        fill_toggle = False
        for name, price, pos, gap in comp_data:
            fill = fill_toggle
            if fill: pdf.set_fill_color(248, 248, 248)
            pdf.cell(55, 8, f"  {name}", 1, 0, 'L', fill)
            pdf.cell(45, 8, f"KRW {int(price):,}" if price else "-", 1, 0, 'C', fill)
            pdf.cell(35, 8, pos, 1, 0, 'C', fill)
            if gap == 0:
                pdf.cell(35, 8, "-", 1, 1, 'C', fill)
            else:
                sign = "+" if gap > 0 else ""
                pdf.cell(35, 8, f"{sign}{int(gap):,}", 1, 1, 'C', fill)
            fill_toggle = not fill_toggle

        pdf.ln(8)

        # 분석
        pdf.set_fill_color(255, 243, 224)
        y_box = pdf.get_y()
        pdf.rect(20, y_box, 170, 45, 'F')
        pdf.set_xy(25, y_box + 4)
        pdf.set_font(KFONT, 'B', 11)
        pdf.set_text_color(166, 138, 86)
        pdf.cell(0, 6, T("[ 포지셔닝 분석 ]", "[ Positioning Analysis ]"), ln=True)
        pdf.set_xy(25, pdf.get_y() + 2)
        pdf.set_font(KFONT, '', 9)
        pdf.set_text_color(60, 60, 60)

        if avg_amber and avg_josun:
            gap_pct = (avg_amber - avg_josun) / avg_josun * 100
            if gap_pct < -10:
                analysis = T(
                    f"엠버퓨어힐은 조선 대비 {abs(gap_pct):.1f}% 낮은 가격입니다. 하이엔드 리조트 브랜드 가치를 고려할 때 상향 여지가 충분하며, 박리다매 전략이 장기적 브랜드 희석을 초래할 수 있어 재검토가 필요합니다.",
                    f"Amber is {abs(gap_pct):.1f}% below Josun. Repositioning needed."
                )
            elif gap_pct < 0:
                analysis = T(
                    f"엠버퓨어힐은 조선 대비 {abs(gap_pct):.1f}% 낮아 적정 프리미엄 포지셔닝이 가능합니다. 시장 강세 구간에서 격차를 좁히는 전략이 유효합니다.",
                    f"Amber is {abs(gap_pct):.1f}% below Josun - room for premium."
                )
            else:
                analysis = T(
                    f"엠버퓨어힐은 조선을 상회하는 프리미엄 포지셔닝을 확보했습니다. 이 위상을 유지하며 ADR을 더 끌어올리는 전략이 필요합니다.",
                    f"Amber is premium-positioned above Josun."
                )
            _safe_multicell(pdf, 160, 5, analysis)

    # ============ 페이지 6: 신뢰도 ============
    pdf.add_page()
    pdf.set_text_color(26, 42, 68)
    pdf.set_font(KFONT, 'B', 20)
    pdf.cell(0, 12, T("05. 시뮬레이터 신뢰도", "05. SIMULATOR CREDIBILITY"), ln=True)
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(20, 32, 30, 2, 'F')
    pdf.ln(8)

    pdf.set_font(KFONT, '', 10)
    pdf.set_text_color(80, 80, 80)
    _safe_multicell(pdf, 0, 6, T(
        "보고서의 결론이 신뢰할 근거에 기반함을 증명하기 위해 시뮬레이터를 3가지 방식으로 자체 검증합니다.",
        "Three-way accuracy verification."
    ))
    pdf.ln(5)

    pdf.set_font(KFONT, 'B', 12)
    pdf.set_text_color(166, 138, 86)
    pdf.cell(0, 8, T("> 검증 방법론", "> Methodology"), ln=True)
    pdf.set_font(KFONT, '', 10)
    pdf.set_text_color(0, 0, 0)

    methods = [
        (T("A. 예약 집중도", "A. Booking Concentration"),
         T("시뮬이 BAR 상향 제안한 날짜에 실제 예약이 증가했는가를 측정합니다.",
           "Measures if bookings increased on flagged dates.")),
        (T("B. 마감 매출", "B. Final Revenue"),
         T("제안 날짜가 평균 이상의 점유율로 마감되었는가를 평가합니다.",
           "Checks if flagged dates closed with high occupancy.")),
        (T("C. 점유율 일치", "C. Occupancy Match"),
         T("제안 BAR 상향 단계와 실제 수요 수준이 일치하는가를 측정합니다.",
           "Measures alignment between suggestion and actual demand.")),
    ]

    for title_m, desc in methods:
        pdf.set_font(KFONT, 'B', 10)
        pdf.set_text_color(26, 42, 68)
        _safe_multicell(pdf, 0, 6, title_m)
        pdf.set_font(KFONT, '', 9)
        pdf.set_text_color(80, 80, 80)
        _safe_multicell(pdf, 0, 5, desc)
        pdf.ln(2)

    pdf.ln(3)

    # 검증 결과
    if snapshots_list:
        try:
            verif = verify_simulator_accuracy(
                snapshots_list, df_flight, df_comp,
                josun_threshold, flight_threshold,
                events, sensitivity, active_search_date
            )
            if verif.get('verification_ready'):
                a = verif['a_score']
                b = verif['b_score']
                c = verif['c_score']
                overall = (a + b + c) / 3

                pdf.set_fill_color(13, 71, 161)
                y_box = pdf.get_y()
                pdf.rect(20, y_box, 170, 40, 'F')
                pdf.set_xy(20, y_box + 5)
                pdf.set_font(KFONT, '', 11)
                pdf.set_text_color(200, 220, 255)
                pdf.cell(0, 7, T("시뮬레이터 종합 정확도", "Overall Accuracy"), ln=True, align='C')
                pdf.set_font(KFONT, 'B', 28)
                pdf.set_text_color(255, 215, 0)
                pdf.cell(0, 15, f"{overall:.1f}%", ln=True, align='C')
                pdf.set_font(KFONT, '', 9)
                pdf.set_text_color(200, 220, 255)
                pdf.cell(0, 5, f"A: {a:.1f}% | B: {b:.1f}% | C: {c:.1f}%", ln=True, align='C')
        except:
            pass
    else:
        pdf.set_fill_color(240, 240, 240)
        y_box = pdf.get_y()
        pdf.rect(20, y_box, 170, 20, 'F')
        pdf.set_xy(25, y_box + 5)
        pdf.set_font(KFONT, '', 10)
        pdf.set_text_color(100, 100, 100)
        _safe_multicell(pdf, 160, 5, T(
            "정확도 데이터는 일일 스냅샷이 2주 이상 축적된 후 포함됩니다.",
            "Accuracy data available after 2+ weeks of snapshots."
        ))

    # ============ 페이지 7: Action Plan ============
    pdf.add_page()
    pdf.set_text_color(26, 42, 68)
    pdf.set_font(KFONT, 'B', 20)
    pdf.cell(0, 12, T("06. 전략적 실행 계획", "06. STRATEGIC ACTION PLAN"), ln=True)
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(20, 32, 30, 2, 'F')
    pdf.ln(8)

    # 문제점
    pdf.set_font(KFONT, 'B', 13)
    pdf.set_text_color(198, 40, 40)
    pdf.cell(0, 8, T("[ 현재 정책의 3가지 문제점 ]", "[ Three Key Issues ]"), ln=True)
    pdf.ln(2)

    issues = [
        (T("1. 점유율 편향", "1. Occupancy Bias"),
         T("점유율 채우기 중심 정책은 단기 판매량은 보장하나 시장 강세 구간의 ADR 상향 기회를 놓쳐 장기 수익성을 저해합니다.",
           "Occupancy focus misses ADR upside during peak demand.")),
        (T("2. 브랜드 가치 희석", "2. Brand Dilution"),
         T("경쟁사 대비 낮은 가격 전략은 하이엔드 리조트로서의 브랜드 포지셔닝을 약화시키고 프리미엄 고객층 이탈을 초래할 수 있습니다.",
           "Below-market pricing dilutes brand positioning.")),
        (T("3. 시장 시그널 무시", "3. Market Signal Blindness"),
         T("경쟁사 가격 변동, 항공권 수요 급증 등 외부 시그널을 가격에 반영하지 않아 기회비용이 지속 누적되고 있습니다.",
           "External signals ignored, causing continuous loss.")),
    ]
    for title_i, desc in issues:
        pdf.set_font(KFONT, 'B', 10)
        pdf.set_text_color(198, 40, 40)
        _safe_multicell(pdf, 0, 6, title_i)
        pdf.set_font(KFONT, '', 9)
        pdf.set_text_color(60, 60, 60)
        _safe_multicell(pdf, 0, 5, desc)
        pdf.ln(2)

    pdf.ln(5)

    # 로드맵
    pdf.set_font(KFONT, 'B', 13)
    pdf.set_text_color(46, 125, 50)
    pdf.cell(0, 8, T("[ 3단계 실행 로드맵 ]", "[ 3-Phase Roadmap ]"), ln=True)
    pdf.ln(2)

    phases = [
        (T("단기 (1주 이내)", "Short-term (1 week)"),
         T("시그널 발동 날짜의 ADR 즉시 검토 및 조정. Dynamic 5개 객실 타입에 대한 일일 가격 모니터링 가동.",
           "Review ADR for flagged dates. Daily monitoring.")),
        (T("중기 (1개월)", "Mid-term (1 month)"),
         T("객실별 민감도 계수 세밀 조정. 전체 점유율 75%/85% 자동 알림 체계 연동. 주간 경영진 보고 정례화.",
           "Fine-tune sensitivity. Automate alerts. Weekly reports.")),
        (T("장기 (1분기)", "Long-term (1 quarter)"),
         T("경쟁사 벤치마크 기반 포지셔닝 재정립. 70~80% 점유율 + 프리미엄 ADR 구조로 순수익 극대화.",
           "Reposition based on benchmarks. Establish premium ADR.")),
    ]
    for title_p, desc in phases:
        pdf.set_font(KFONT, 'B', 10)
        pdf.set_text_color(46, 125, 50)
        _safe_multicell(pdf, 0, 6, title_p)
        pdf.set_font(KFONT, '', 9)
        pdf.set_text_color(60, 60, 60)
        _safe_multicell(pdf, 0, 5, desc)
        pdf.ln(2)

    # ============ 페이지 8: 결론 ============
    pdf.add_page()
    pdf.set_text_color(26, 42, 68)
    pdf.set_font(KFONT, 'B', 20)
    pdf.cell(0, 12, T("07. 결론 및 제언", "07. CONCLUSION"), ln=True)
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(20, 32, 30, 2, 'F')
    pdf.ln(10)

    if not opp_df.empty:
        positive_opp = opp_df[opp_df['기회비용'] > 0]['기회비용'].sum()
        pdf.set_fill_color(26, 42, 68)
        pdf.rect(20, pdf.get_y(), 170, 55, 'F')
        pdf.set_xy(20, pdf.get_y() + 8)
        pdf.set_font(KFONT, 'B', 12)
        pdf.set_text_color(255, 255, 255)
        pdf.cell(0, 7, T("누적 기회비용", "Total Opportunity Cost"), ln=True, align='C')
        pdf.set_font(KFONT, 'B', 32)
        pdf.set_text_color(255, 215, 0)
        pdf.cell(0, 18, f"KRW {int(positive_opp):,}", ln=True, align='C')
        pdf.set_font(KFONT, '', 10)
        pdf.set_text_color(200, 200, 200)
        pdf.cell(0, 5, T("시장 시그널 기반 가격으로 확보 가능했던 매출",
                          "Revenue achievable with signal-based pricing"), ln=True, align='C')

    pdf.ln(15)

    pdf.set_font(KFONT, 'B', 12)
    pdf.set_text_color(166, 138, 86)
    pdf.cell(0, 8, T("[ 핵심 제언 ]", "[ Key Recommendations ]"), ln=True)
    pdf.ln(2)

    pdf.set_font(KFONT, '', 10)
    pdf.set_text_color(40, 40, 40)
    conclusion = T(
        "엠버퓨어힐은 제주 최고급 리조트로서 명확한 브랜드 자산을 보유합니다. 그러나 현재의 점유율 중심 가격 정책은 이 자산을 충분히 화폐화하지 못하고 있습니다.\n\n"
        "본 보고서가 제시하는 시장 시그널 기반 동적 가격 전략은 객실 판매량을 크게 희생하지 않으면서 ADR을 체계적으로 상향시켜 총 매출과 순수익을 동시에 증대시킬 수 있는 검증된 접근법입니다.\n\n"
        "70~80% 점유율 + 프리미엄 ADR 조합은 브랜드 가치를 보호하며 수익을 극대화하는 하이엔드 리조트의 표준 전략이며, 파르나스 제주 등 동급 경쟁사들이 이미 채택하고 있는 정책입니다.\n\n"
        "본 시뮬레이터와 실행 로드맵을 통해 엠버퓨어힐의 수익 구조를 한 단계 업그레이드할 수 있기를 제언드립니다.",
        "Amber Pure Hill holds strong brand equity. Current pricing doesn't fully monetize it.\n\n"
        "Market signal-based dynamic pricing can systematically raise ADR while maintaining occupancy.\n\n"
        "The 70-80% OCC + premium ADR strategy is the standard for high-end resorts.\n\n"
        "We recommend adopting this simulator and roadmap."
    )
    _safe_multicell(pdf, 0, 6, conclusion)

    # 푸터
    pdf.set_y(275)
    pdf.set_font(KFONT, '', 8)
    pdf.set_text_color(150, 150, 150)
    pdf.cell(0, 10, T("대외비 - 엠버퓨어힐 전략 수익 분석",
                       "CONFIDENTIAL - AMBER PURE HILL"), 0, 0, 'C')

    return bytes(pdf.output())

# ============================================================
# 23. 세션 초기화
# ============================================================
if 'cmd_today_df' not in st.session_state: st.session_state.cmd_today_df = pd.DataFrame()
if 'cmd_prev_df' not in st.session_state: st.session_state.cmd_prev_df = pd.DataFrame()
if 'cmd_compare_label' not in st.session_state: st.session_state.cmd_compare_label = ""
    
df_flight_all, df_comp_all = load_market_data()
all_notes = load_all_notes()
all_events = load_events()
sensitivity = load_sensitivity()

# ============================================================
# 24. UI - 사이드바
# ============================================================
st.title("🏨 Amber Command Center")
st.caption(f"v8.0 · 오늘 기준일: {TODAY.strftime('%Y-%m-%d')} ({WEEKDAYS_KR[TODAY.weekday()]})")

with st.sidebar:
    st.header("📅 과거 기록 조회")
    try:
        if db_hotel:
            all_docs = db_hotel.collection("daily_snapshots").select(["work_date"]).stream()
        saved_dates = sorted(list(set([d.to_dict().get('work_date', '') for d in all_docs if d.to_dict().get('work_date')])))
        if saved_dates:
            st.markdown("**📌 저장된 날짜 (최근 14일)**")
            tags = "".join([
                f"<span style='background:#E8F5E9; border:1px solid #4CAF50; color:#2E7D32; padding:3px 8px; border-radius:12px; margin:2px; font-size:12px; display:inline-block;'>{d[5:]} ✅</span>"
                for d in saved_dates[-14:]
            ])
            st.markdown(f"<div style='margin-bottom:10px;'>{tags}</div>", unsafe_allow_html=True)
    except: pass

    work_day = st.date_input("조회 날짜", value=date.today())
    if st.button("📂 과거 기록 불러오기"):
        if db_hotel:  # ← 추가
            docs = db_hotel.collection("daily_snapshots").where("work_date", "==", work_day.strftime("%Y-%m-%d")).limit(1).stream()
            found = False
            for doc in docs:
                d_dict = doc.to_dict()
                st.session_state.cmd_today_df = pd.DataFrame(d_dict['data'])
                if not st.session_state.cmd_today_df.empty and 'Date' in st.session_state.cmd_today_df.columns:
                    st.session_state.cmd_today_df['Date'] = pd.to_datetime(st.session_state.cmd_today_df['Date']).dt.date
                if 'prev_data' in d_dict and d_dict['prev_data']:
                    st.session_state.cmd_prev_df = pd.DataFrame(d_dict['prev_data'])
                    if not st.session_state.cmd_prev_df.empty and 'Date' in st.session_state.cmd_prev_df.columns:
                        st.session_state.cmd_prev_df['Date'] = pd.to_datetime(st.session_state.cmd_prev_df['Date']).dt.date
                else:
                    st.session_state.cmd_prev_df = pd.DataFrame()
                st.session_state.cmd_compare_label = f"불러온 과거 기록: {work_day}"
                found = True

    st.divider()
    st.header("🎯 시뮬레이터 기준값")
    josun_threshold = st.number_input("그랜드 조선 임계가", min_value=100000, max_value=1000000, value=400000, step=10000)
    flight_threshold = st.number_input("항공권 임계가", min_value=10000, max_value=300000, value=70000, step=5000)

    search_date_options = []
    if not df_flight_all.empty and 'search_date_str' in df_flight_all.columns:
        search_date_options = sorted(df_flight_all['search_date_str'].dropna().unique().tolist(), reverse=True)
    search_date_options = [x for x in search_date_options if x]
    selected_search_date = st.selectbox("🗓️ 시장 데이터 수집일", ["최신"] + search_date_options)
    active_search_date = None if selected_search_date == "최신" else selected_search_date

    st.divider()

    with st.expander("🎯 객실별 민감도", expanded=False):
        st.caption("1.0=기본 / 0.5=둔감 / 1.5=민감")
        new_sens = {}
        for rid in DYNAMIC_ROOMS:
            new_sens[rid] = st.slider(
                f"{rid}", min_value=0.3, max_value=2.0,
                value=float(sensitivity.get(rid, 1.0)), step=0.1, key=f"sens_{rid}"
            )
        if st.button("💾 민감도 저장"):
            if save_sensitivity(new_sens):
                st.success("저장 완료!")
                st.rerun()

    st.divider()
    st.header("🔄 요금 에디터 연동")
    st.info("요금 에디터 사이트에서 업로드하고 확정한 최신 데이터를 불러옵니다. (파일 별도 업로드 불필요)")
    
    if st.button("🚀 최신 데이터 동기화", type="primary", use_container_width=True):
        with st.spinner("요금 에디터 데이터 불러오는 중..."):
        
            st.cache_data.clear() # ✨ 핵심: 동기화 누를 때 기존 캐시(메모리) 강제 삭제!
          
            latest_db, save_dt = get_latest_snapshot(db_hotel)
        
            if not latest_db.empty:
                st.session_state.cmd_today_df = latest_db.sort_values(by=['Date', 'RoomID'])
            
                # 어제 데이터(비교군) 가져오기 로직
                if db_hotel:  # ← 🔴 변경 1: 안전장치 추가
                    docs = db_hotel.collection("daily_snapshots").where("work_date", "<", date.today().strftime("%Y-%m-%d")).order_by("work_date", direction=firestore.Query.DESCENDING).limit(1).stream()  # ← 🔴 변경 2: db → db_hotel
                    prev_found = False
                    for doc in docs:
                        p_dict = doc.to_dict()
                        p_df = pd.DataFrame(p_dict.get('data', []))
                        if not p_df.empty and 'Date' in p_df.columns:
                            p_df['Date'] = pd.to_datetime(p_df['Date']).dt.date
                            st.session_state.cmd_prev_df = p_df
                            prev_found = True
                
                    if not prev_found:
                        st.session_state.cmd_prev_df = pd.DataFrame()
                else:  # ← 🔴 변경 3: db_hotel이 None일 때 대비
                    st.session_state.cmd_prev_df = pd.DataFrame()
                
                st.session_state.cmd_compare_label = f"✅ 동기화 완료 (요금 에디터 기준: {save_dt})"
                st.rerun()
            else:
                st.warning("요금 에디터에 등록된 최신 데이터가 없습니다.")

# ============================================================
# 메인 영역
# ============================================================
if not st.session_state.cmd_today_df.empty:
    curr = st.session_state.cmd_today_df
    prev = st.session_state.cmd_prev_df

    alert_opp_df = calculate_opportunity_cost(
        curr, df_flight_all, df_comp_all,
        josun_threshold, flight_threshold,
        active_search_date, events=all_events, sensitivity=sensitivity
    )

    # 🚨 상단 Dynamic 알림 (미래만)
    alerts = generate_today_alerts(alert_opp_df, all_notes, all_events, top_n=5, only_future=True)
    if alerts:
        st.markdown(f"""
        <div style='background: linear-gradient(90deg, #FF6B6B 0%, #FFA500 100%); 
                    padding: 12px 20px; border-radius: 10px; color: white; margin-bottom: 10px;'>
            <div style='font-size: 20px; font-weight: bold;'>🚨 오늘 주목해야 할 미래 날짜 TOP 5</div>
            <div style='font-size: 12px; opacity: 0.9;'>오늘({TODAY.strftime('%m/%d')}) 이후 시그널 발동 중인 날짜</div>
        </div>
        """, unsafe_allow_html=True)
        alert_cols = st.columns(len(alerts))
        for idx, a in enumerate(alerts):
            with alert_cols[idx]:
                note_icon = "📝" if a['메모있음'] else "⚪"
                ev_icon = f"🎉{a['이벤트'][:5]}" if a['이벤트'] else ""
                bg_color = "#FFEBEE" if a['BAR상승'] >= 2 else "#FFF3E0"
                border_color = "#C62828" if a['BAR상승'] >= 2 else "#FF6F00"
                days_until = (a['날짜'] - TODAY).days
                urgency = f"D-{days_until}" if days_until > 0 else "오늘"
                st.markdown(f"""
                <div style='background:{bg_color}; border:2px solid {border_color}; 
                            border-radius:10px; padding:12px; min-height:150px;'>
                    <div style='font-size:11px; color:#888; font-weight:bold;'>{urgency}</div>
                    <div style='font-size:13px; color:#666; font-weight:bold;'>
                        {a['날짜'].strftime('%m/%d')} ({a['요일']}) {note_icon}
                    </div>
                    <div style='font-size:22px; color:{border_color}; font-weight:bold; margin:5px 0;'>
                        BAR +{a['BAR상승']}
                    </div>
                    <div style='font-size:11px; color:#444;'>{a['시그널'][:25]}{ev_icon}</div>
                    <div style='font-size:13px; color:#D32F2F; font-weight:bold; margin-top:6px;'>
                        ₩{a['기회비용']:,}
                    </div>
                    {f"<div style='font-size:10px; color:#888; margin-top:4px;'>📝 {a['메모미리보기']}...</div>" if a['메모있음'] else ""}
                </div>""", unsafe_allow_html=True)
        st.markdown("")

    # 🏨 Fixed 수기 인상 (미래만)
    fixed_alerts_future, fixed_alerts_past = get_fixed_room_alerts(curr, all_events)
    if fixed_alerts_future:
        critical = [a for a in fixed_alerts_future if a['level'] == 'critical']
        warning = [a for a in fixed_alerts_future if a['level'] == 'warning']

        st.markdown(f"""
        <div style='background: linear-gradient(90deg, #7B1FA2 0%, #9C27B0 100%); 
                    padding: 12px 20px; border-radius: 10px; color: white; margin-bottom: 10px;'>
            <div style='font-size: 18px; font-weight: bold;'>🏨 Fixed 객실 수기 인상 알림 (미래)</div>
            <div style='font-size: 12px; opacity: 0.9;'>오늘 이후 전체 점유율 기준</div>
        </div>
        """, unsafe_allow_html=True)

        if critical:
            st.markdown("**🚨 강력 권장 (85%↑)**")
            for a in critical:
                ev_txt = f"🎉 {a['이벤트']}" if a['이벤트'] else ""
                days_until = (a['날짜'] - TODAY).days
                urgency = f"D-{days_until}" if days_until > 0 else "오늘"
                st.markdown(f"""
                <div style='background:#FFEBEE; border-left:5px solid #C62828; padding:10px; border-radius:6px; margin-bottom:5px;'>
                    <b>[{urgency}] {a['날짜'].strftime('%Y-%m-%d')} ({a['요일']})</b> 
                    · 전체점유율 <b style='color:#C62828;'>{a['전체점유율']}%</b> 
                    · {a['메시지']} {ev_txt}
                </div>""", unsafe_allow_html=True)

        if warning:
            st.markdown("**🔔 검토 권장 (75%↑)**")
            for a in warning:
                ev_txt = f"🎉 {a['이벤트']}" if a['이벤트'] else ""
                days_until = (a['날짜'] - TODAY).days
                urgency = f"D-{days_until}" if days_until > 0 else "오늘"
                st.markdown(f"""
                <div style='background:#FFF3E0; border-left:5px solid #FF6F00; padding:10px; border-radius:6px; margin-bottom:5px;'>
                    <b>[{urgency}] {a['날짜'].strftime('%Y-%m-%d')} ({a['요일']})</b> 
                    · 전체점유율 <b style='color:#FF6F00;'>{a['전체점유율']}%</b> 
                    · {a['메시지']} {ev_txt}
                </div>""", unsafe_allow_html=True)

        st.markdown("")

    # 📜 과거 기록 (접힌 상태)
    if fixed_alerts_past:
        with st.expander(f"📜 마감된 과거 기록 {len(fixed_alerts_past)}건 보기", expanded=False):
            st.caption("이미 지나간 날짜들 - 당시 수기 인상 대상이었던 날들의 기록")
            for a in fixed_alerts_past[-10:]:
                level_color = "#C62828" if a['level'] == 'critical' else "#FF6F00"
                st.markdown(f"""
                <div style='background:#F5F5F5; border-left:3px solid {level_color}; padding:8px; border-radius:4px; margin-bottom:4px; opacity:0.7;'>
                    <b>{a['날짜'].strftime('%Y-%m-%d')} ({a['요일']})</b> 
                    · 당시 점유율 {a['전체점유율']}% · {a['단계']} (마감됨)
                </div>""", unsafe_allow_html=True)

    if st.session_state.cmd_compare_label:
        st.info(f"ℹ️ {st.session_state.cmd_compare_label}")

    # ----------------------------------------------------
    # [신규 추가] 표 표시용 날짜 필터 (탭 메뉴 바로 위에 배치)
    # ----------------------------------------------------
    all_dates = sorted(curr['Date'].unique())
    st.markdown(f"""
    <div style='background:#E3F2FD; padding:10px 15px; border-radius:8px; margin:15px 0; 
                border-left:5px solid #1976D2;'>
        📅 <b>오늘 기준:</b> {TODAY.strftime('%Y-%m-%d')} ({WEEKDAYS_KR[TODAY.weekday()]}) · 
        기본적으로 <b>오늘 이후</b>만 표시됩니다. 과거를 보려면 아래 체크박스를 활성화하세요.
    </div>
    """, unsafe_allow_html=True)
    
    visible_dates = date_filter_toggle("v8_main", all_dates, default_show_past=False)
    curr_ui = filter_df_by_dates(curr, visible_dates)
    prev_ui = filter_df_by_dates(prev, visible_dates) if not prev.empty else prev

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11 = st.tabs([
        "📊 현황",
        "🔮 시뮬레이터",
        "💸 기회비용",
        "📈 시장 트렌드",
        "📊 누적 추적",
        "🔄 정확도",
        "📅 주간 요약",
        "🎯 할 일",
        "📊 전년 비교",
        "📄 PDF",
        "💡 종합 인사이트" # ✨ 11번째 탭 추가
    ])

    # =============== TAB 1: 현황 ===============
    with tab1:
        applied_rates_data = load_applied_rates()
        
        st.markdown(render_master_table(curr_ui, prev_ui, applied_rates=applied_rates_data, title="🎯 1. 통합 관제 보드 (권장 vs 수동개입 vs 최종요금)", mode="통합관제"), unsafe_allow_html=True)
        st.markdown(render_master_table(curr_ui, prev_ui, title="📈 2. 예약 변화량 (픽업)", mode="변화"), unsafe_allow_html=True)
        
        # 덜 중요한 원시 데이터들은 접어둡니다.
        with st.expander("🔍 순수 시스템 권장가 및 판도 변화 보기 (원시 데이터)"):
            st.markdown(render_master_table(curr_ui, prev_ui, title="📊 시장 분석 (순수 시스템 권장)", mode="기준"), unsafe_allow_html=True)
            st.markdown(render_master_table(curr_ui, prev_ui, title="🔔 판도 변화", mode="판도변화"), unsafe_allow_html=True)

        st.divider()
        st.subheader("📝 날짜별 메모")
        dates_list = sorted(curr['Date'].unique())
        future_dates = [d for d in dates_list if d >= TODAY]
        if future_dates:
            selected_date_memo = st.selectbox(
                "메모할 날짜 (미래)", future_dates,
                format_func=lambda d: f"{d.strftime('%Y-%m-%d')} ({WEEKDAYS_KR[d.weekday()]})",
                key="tab1_memo_date"
            )
            note_key = get_note_key(selected_date_memo)
            render_note_input(note_key, f"{selected_date_memo.strftime('%m/%d')} 메모", all_notes, widget_prefix="tab1")
        else:
            st.info("미래 날짜 데이터가 없습니다.")

    # =============== TAB 2: 시뮬레이터 ===============
    with tab2:
        st.subheader("🔮 시장 시그널 반영 BAR 제안")
        col_info1, col_info2, col_info3 = st.columns(3)
        with col_info1:
            st.info(f"🏨 조선: {josun_threshold:,}원↑ → +1")
        with col_info2:
            st.info(f"✈️ 항공: {flight_threshold:,}원↑ → +1")
        with col_info3:
            st.info(f"📜 회색 = 이미 마감된 날짜")

        sim_html = render_sim_comparison_table(
            curr_ui, df_flight_all, df_comp_all,
            josun_threshold, flight_threshold,
            active_search_date, events=all_events, sensitivity=sensitivity
        )
        st.markdown(sim_html, unsafe_allow_html=True)

        st.divider()
        st.subheader("📝 시그널 발동 날짜 메모 (미래만)")
        triggered_future = alert_opp_df[(alert_opp_df['BAR상승'] > 0) & (alert_opp_df['날짜'] >= TODAY)] if not alert_opp_df.empty else pd.DataFrame()
        if not triggered_future.empty:
            triggered_dates = sorted(triggered_future['날짜'].unique())
            sel_trigger_date = st.selectbox(
                "메모할 날짜", triggered_dates,
                format_func=lambda d: f"{d.strftime('%Y-%m-%d')} ({WEEKDAYS_KR[d.weekday()]})",
                key="tab2_memo_date"
            )
            note_key = get_note_key(sel_trigger_date)
            render_note_input(note_key, f"{sel_trigger_date.strftime('%m/%d')} 의사결정", all_notes, widget_prefix="tab2")
        else:
            st.info("미래에 시그널 발동 중인 날짜가 없습니다.")

    # =============== TAB 3: 기회비용 ===============
    with tab3:
        st.subheader("💸 기회비용 분석")

        view_mode = st.radio(
            "보기 모드",
            ["🔮 미래 (오늘 이후)", "📜 과거 (마감 기록)", "📊 전체"],
            horizontal=True
        )

        opp_df = alert_opp_df.copy()
        if view_mode == "🔮 미래 (오늘 이후)":
            opp_df = opp_df[opp_df['날짜'] >= TODAY]
        elif view_mode == "📜 과거 (마감 기록)":
            opp_df = opp_df[opp_df['날짜'] < TODAY]

        if opp_df.empty:
            st.info("해당 범위에 데이터가 없습니다.")
        else:
            positive_opp = opp_df[opp_df['기회비용'] > 0]['기회비용'].sum()
            boosted_count = (opp_df['BAR상승'] > 0).sum()
            days_affected = opp_df[opp_df['기회비용'] > 0]['날짜'].nunique()
            st.markdown(f"""
            <div style='background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); 
                        padding: 30px; border-radius: 15px; color: white; text-align: center; margin-bottom: 20px;'>
                <div style='font-size: 16px; color: #80D8FF;'>{view_mode} 추정 추가 수익</div>
                <div style='font-size: 42px; font-weight: bold; color: #FFD700;'>₩ {int(positive_opp):,}</div>
            </div>
            """, unsafe_allow_html=True)
            m1, m2, m3 = st.columns(3)
            m1.metric("시그널 발동", f"{boosted_count}건")
            m2.metric("영향 날짜", f"{days_affected}일")
            m3.metric("객실타입", f"{opp_df['객실타입'].nunique()}개")

            st.divider()
            daily_opp = opp_df.groupby(['날짜', '요일'])['기회비용'].sum().reset_index()
            daily_opp['라벨'] = daily_opp['날짜'].apply(lambda x: x.strftime('%m-%d')) + '(' + daily_opp['요일'] + ')'
            fig1 = px.bar(daily_opp, x='라벨', y='기회비용', color='기회비용',
                          color_continuous_scale=['#E8F5E9', '#FF5252'])
            fig1.update_layout(template="plotly_white", height=400, xaxis_tickangle=-45, showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)

            st.subheader("🔥 TOP 10")
            top10 = opp_df[opp_df['기회비용'] > 0].nlargest(10, '기회비용').copy()
            top10['메모'] = top10['날짜'].apply(lambda d: "📝" if get_note_key(d) in all_notes else "⚪")
            top10['날짜'] = top10['날짜'].apply(lambda x: x.strftime('%Y-%m-%d'))
            for col in ['실제단가', '시뮬단가', '단가차이', '기회비용']:
                top10[col] = top10[col].apply(lambda x: f"₩{int(x):,}")
            st.dataframe(top10[['날짜', '요일', '객실타입', '판매객실수', '실제BAR',
                                '시뮬BAR', '기회비용', '시그널', '메모']],
                         use_container_width=True, hide_index=True)

    # =============== TAB 4: 시장 트렌드 ===============
    with tab4:
        st.subheader("📈 시장 가격 트렌드")
        if df_flight_all.empty and df_comp_all.empty:
            st.warning("크롤링 데이터가 없습니다.")
        else:
            dates_list = sorted(curr['Date'].unique())
            trend_data = []
            our_prices = get_our_avg_price_for_dates(curr, dates_list)
            for d in dates_list:
                josun_p, parnas_p, flight_p = get_market_price_for_date(d, df_flight_all, df_comp_all, active_search_date)
                amber_crawl_p = None
                if not df_comp_all.empty:
                    c_row = df_comp_all[df_comp_all['date'] == d]
                    if active_search_date and 'search_date_str' in c_row.columns:
                        c_row = c_row[c_row['search_date_str'] == active_search_date]
                    amber_rows = c_row[c_row['hotel_name'].str.contains('Amber', case=False, na=False)]
                    if not amber_rows.empty:
                        amber_crawl_p = amber_rows['price'].min()
                trend_data.append({
                    '날짜': d, '그랜드조선': josun_p, '파르나스': parnas_p,
                    '엠버_크롤링': amber_crawl_p, '엠버_시스템': our_prices.get(d), '항공권': flight_p,
                })
            trend_df = pd.DataFrame(trend_data)
            trend_df['날짜_dt'] = pd.to_datetime(trend_df['날짜'])

            fig_hotel = go.Figure()
            if trend_df['그랜드조선'].notna().any():
                fig_hotel.add_trace(go.Scatter(x=trend_df['날짜_dt'], y=trend_df['그랜드조선'],
                    mode='lines+markers', name='조선', line=dict(color='#2E7D32', width=3)))
            if trend_df['파르나스'].notna().any():
                fig_hotel.add_trace(go.Scatter(x=trend_df['날짜_dt'], y=trend_df['파르나스'],
                    mode='lines+markers', name='파르나스', line=dict(color='#9C27B0', width=2, dash='dot')))
            if trend_df['엠버_크롤링'].notna().any():
                fig_hotel.add_trace(go.Scatter(x=trend_df['날짜_dt'], y=trend_df['엠버_크롤링'],
                    mode='lines+markers', name='엠버(크롤링)', line=dict(color='#D32F2F', width=3)))
            if trend_df['엠버_시스템'].notna().any():
                fig_hotel.add_trace(go.Scatter(x=trend_df['날짜_dt'], y=trend_df['엠버_시스템'],
                    mode='lines+markers', name='엠버(시스템)', line=dict(color='#1976D2', width=3)))
            fig_hotel.add_hline(y=josun_threshold, line=dict(color='red', dash='dash'))
            fig_hotel.add_shape(
                type="line",
                x0=TODAY, x1=TODAY,
                y0=0, y1=1,
                yref="paper",
                line=dict(color='purple', dash='dot', width=2)
            )
            fig_hotel.add_annotation(
                x=TODAY, y=1, yref="paper",
                text="오늘", showarrow=False,
                font=dict(color='purple', size=11),
                yshift=10
            )
            fig_hotel.update_layout(template="plotly_white", height=500, hovermode='x unified')
            fig_hotel.update_yaxes(tickformat=",")
            st.plotly_chart(fig_hotel, use_container_width=True)

            fig_flight = go.Figure()
            if trend_df['항공권'].notna().any():
                colors = ['#D32F2F' if (pd.notna(p) and p and p >= flight_threshold) else '#90A4AE'
                          for p in trend_df['항공권']]
                fig_flight.add_trace(go.Bar(x=trend_df['날짜_dt'], y=trend_df['항공권'],
                    marker_color=colors,
                    text=[f"{int(p):,}" if (pd.notna(p) and p) else "" for p in trend_df['항공권']],
                    textposition='outside'))
            fig_flight.add_hline(y=flight_threshold, line=dict(color='red', dash='dash'))
            fig_flight.add_shape(
                type="line",
                x0=TODAY, x1=TODAY,
                y0=0, y1=1,
                yref="paper",
                line=dict(color='purple', dash='dot', width=2)
            )
            fig_flight.add_annotation(
                x=TODAY, y=1, yref="paper",
                text="오늘", showarrow=False,
                font=dict(color='purple', size=11),
                yshift=10
            )
            fig_flight.update_layout(template="plotly_white", height=400, showlegend=False)
            fig_flight.update_yaxes(tickformat=",")
            st.plotly_chart(fig_flight, use_container_width=True)

    # =============== TAB 5: 누적 추적 ===============
    with tab5:
        st.subheader("📊 누적 기회비용 추적")
        days_back = st.radio("조회 기간", [30, 60, 90], index=0, horizontal=True)

        with st.spinner(f"최근 {days_back}일 분석 중..."):
            snapshots_list = load_all_snapshots_history(days_back=days_back)
            cum_df = calculate_cumulative_metrics(
                snapshots_list, df_flight_all, df_comp_all,
                josun_threshold, flight_threshold,
                all_events, sensitivity, active_search_date
            )

        if cum_df.empty:
            st.info(f"📭 최근 {days_back}일 누적 데이터가 부족합니다.")
        else:
            total_cum = cum_df['opportunity_cost'].sum()
            avg_daily = cum_df['opportunity_cost'].mean()
            total_triggers = cum_df['trigger_count'].sum()

            st.markdown(f"""
            <div style='background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); 
                        padding: 30px; border-radius: 15px; color: white; text-align: center; margin-bottom: 20px;'>
                <div style='font-size: 16px; color: #80D8FF;'>최근 {days_back}일 누적 기회비용</div>
                <div style='font-size: 42px; font-weight: bold; color: #FFD700;'>₩ {int(total_cum):,}</div>
                <div style='font-size: 12px; color: #bbb; margin-top: 10px;'>
                    일평균 ₩{int(avg_daily):,} · 총 시그널 {int(total_triggers)}건
                </div>
            </div>""", unsafe_allow_html=True)

            cum_df['work_date_dt'] = pd.to_datetime(cum_df['work_date'])
            cum_df = cum_df.sort_values('work_date_dt')
            cum_df['누적_기회비용'] = cum_df['opportunity_cost'].cumsum()

            fig_cum = go.Figure()
            fig_cum.add_trace(go.Scatter(
                x=cum_df['work_date_dt'], y=cum_df['누적_기회비용'],
                mode='lines+markers', name='누적', fill='tozeroy',
                line=dict(color='#FFD700', width=3), fillcolor='rgba(255, 215, 0, 0.15)'
            ))
            fig_cum.update_layout(template="plotly_white", height=400, title="누적 기회비용 추이",
                                   yaxis_title="누적 (원)", hovermode='x unified')
            fig_cum.update_yaxes(tickformat=",")
            st.plotly_chart(fig_cum, use_container_width=True)


        # =============== TAB 6: 정확도 ===============
    with tab6:
        st.subheader("🔄 시뮬레이터 정확도 검증")
        with st.spinner("검증 중..."):
            snapshots_list = load_all_snapshots_history(days_back=90)
            verification = verify_simulator_accuracy(
                snapshots_list, df_flight_all, df_comp_all,
                josun_threshold, flight_threshold,
                all_events, sensitivity, active_search_date
            )

        if not verification.get('verification_ready'):
            st.info(f"📭 {verification.get('message', '검증 불가')}")
        else:
            a_score = verification['a_score']
            b_score = verification['b_score']
            c_score = verification['c_score']
            overall = (a_score + b_score + c_score) / 3

            st.markdown(f"""
            <div style='background: linear-gradient(135deg, #0D47A1 0%, #1565C0 100%); 
                        padding: 30px; border-radius: 15px; color: white; text-align: center; margin-bottom: 20px;'>
                <div style='font-size: 16px; color: #BBDEFB;'>시뮬레이터 종합 정확도</div>
                <div style='font-size: 48px; font-weight: bold; color: #FFD700;'>{overall:.1f}%</div>
            </div>""", unsafe_allow_html=True)

            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("🎯 A. 예약 집중도", f"{a_score:.1f}%",
                          f"{verification['a_correct']} / {verification['a_total']}")
            with c2:
                st.metric("💰 B. 마감 매출", f"{b_score:.1f}%",
                          f"{verification['b_correct']} / {verification['b_total']}")
            with c3:
                st.metric("📊 C. 점유율 일치", f"{c_score:.1f}%",
                          f"{verification['c_correct']} / {verification['c_total']}")

            st.success(f"""
            💡 **회장님 보고용 멘트**: 
            "엠버퓨어힐 자체 수익관리 시뮬레이터는 현재 **{overall:.0f}%의 종합 정확도**를 보이고 있으며, 
            검증 횟수가 쌓일수록 정확도가 더욱 개선될 예정입니다."
            """)

    # =============== TAB 7: 📅 주간 요약 (NEW!) ===============
    with tab7:
        st.subheader("📅 주간 요약 리포트")
        st.caption(f"오늘({TODAY.strftime('%Y-%m-%d')}) 기준 주간 분석")

        summary = generate_weekly_summary(alert_opp_df, all_notes, all_events)

        # 3주 카드
        col_w1, col_w2, col_w3 = st.columns(3)

        with col_w1:
            p = summary['last_week']
            st.markdown(f"""
            <div style='background:#F5F5F5; border-left:5px solid #9E9E9E; padding:20px; border-radius:8px; min-height:200px;'>
                <div style='font-size:14px; color:#666;'>📜 지난 주</div>
                <div style='font-size:13px; color:#888;'>{p['start'].strftime('%m/%d')} ~ {p['end'].strftime('%m/%d')}</div>
                <div style='font-size:24px; font-weight:bold; color:#616161; margin:10px 0;'>₩{int(p['total_opp']):,}</div>
                <div style='font-size:12px; color:#666;'>
                    · 시그널 {p['trigger_count']}건<br>
                    · {p['affected_days']}일 영향
                </div>
            </div>""", unsafe_allow_html=True)

        with col_w2:
            p = summary['this_week']
            st.markdown(f"""
            <div style='background:#FFF3E0; border-left:5px solid #FF6F00; padding:20px; border-radius:8px; min-height:200px;'>
                <div style='font-size:14px; color:#E65100;'>🔔 이번 주</div>
                <div style='font-size:13px; color:#E65100;'>{p['start'].strftime('%m/%d')} ~ {p['end'].strftime('%m/%d')}</div>
                <div style='font-size:28px; font-weight:bold; color:#E65100; margin:10px 0;'>₩{int(p['total_opp']):,}</div>
                <div style='font-size:12px; color:#666;'>
                    · 시그널 {p['trigger_count']}건<br>
                    · {p['affected_days']}일 영향
                </div>
            </div>""", unsafe_allow_html=True)

        with col_w3:
            p = summary['next_week']
            st.markdown(f"""
            <div style='background:#E3F2FD; border-left:5px solid #1976D2; padding:20px; border-radius:8px; min-height:200px;'>
                <div style='font-size:14px; color:#0D47A1;'>🔮 다음 주</div>
                <div style='font-size:13px; color:#0D47A1;'>{p['start'].strftime('%m/%d')} ~ {p['end'].strftime('%m/%d')}</div>
                <div style='font-size:28px; font-weight:bold; color:#0D47A1; margin:10px 0;'>₩{int(p['total_opp']):,}</div>
                <div style='font-size:12px; color:#666;'>
                    · 시그널 {p['trigger_count']}건<br>
                    · {p['affected_days']}일 영향
                </div>
            </div>""", unsafe_allow_html=True)

        st.divider()

        # 각 주차별 TOP 3
        for week_key, week_label, color in [
            ('last_week', '📜 지난 주 TOP 3 누수일', '#9E9E9E'),
            ('this_week', '🔔 이번 주 TOP 3 누수일', '#FF6F00'),
            ('next_week', '🔮 다음 주 TOP 3 주목일', '#1976D2'),
        ]:
            p = summary[week_key]
            st.markdown(f"### {week_label}")
            if p['top_days']:
                for idx, day in enumerate(p['top_days']):
                    d = day['날짜']
                    st.markdown(f"""
                    <div style='background:white; border-left:3px solid {color}; padding:10px 15px; border-radius:4px; margin-bottom:5px; box-shadow:0 1px 3px rgba(0,0,0,0.1);'>
                        <div style='display:flex; justify-content:space-between;'>
                            <span><b>{idx+1}. {d.strftime('%m/%d')} ({day['요일']})</b> · {day['시그널'][:30]}</span>
                            <span style='color:{color}; font-weight:bold;'>₩{int(day['기회비용']):,}</span>
                        </div>
                    </div>""", unsafe_allow_html=True)
            else:
                st.info(f"해당 주차 시그널 발동 없음")
            st.markdown("")

        # 보고용 멘트
        this_week = summary['this_week']
        next_week = summary['next_week']
        st.markdown("---")
        st.markdown("### 💼 이번 주 보고용 멘트 (복사해서 사용)")
        this_top = ', '.join([f"{d['날짜'].strftime('%m/%d')}({d['요일']})" for d in this_week['top_days'][:3]]) if this_week['top_days'] else '없음'
        report_text = f"""**주간 수익관리 리포트 ({TODAY.strftime('%Y-%m-%d')})**

- **지난 주**: 기회비용 ₩{int(summary['last_week']['total_opp']):,} / 시그널 {summary['last_week']['trigger_count']}건
- **이번 주**: 추정 기회비용 ₩{int(this_week['total_opp']):,} / 시그널 {this_week['trigger_count']}건
- **다음 주**: 주목 추정 ₩{int(next_week['total_opp']):,} / 시그널 {next_week['trigger_count']}건

**이번 주 주요 주목일**: {this_top}"""
        st.code(report_text, language=None)

    # =============== TAB 8: 🎯 할 일 체크리스트 (NEW!) ===============
    with tab8:
        st.subheader("🎯 할 일 체크리스트")
        st.caption("의사결정/대기 태그 메모를 체크리스트로 관리합니다.")

        # 필터
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            filter_tag = st.multiselect(
                "태그 필터",
                ['의사결정', '대기', '경고', '일반'],
                default=['의사결정', '대기', '경고']
            )
        with col_f2:
            filter_status = st.radio(
                "상태",
                ['전체', '진행 중만', '완료만'],
                horizontal=True
            )

        # 할 일 리스트 정리
        todo_list = []
        for key, val in all_notes.items():
            if val.get('tag') not in filter_tag:
                continue

            completed = val.get('completed', False)
            if filter_status == '진행 중만' and completed:
                continue
            if filter_status == '완료만' and not completed:
                continue

            # 날짜 파싱
            parts = key.rsplit('_', 1)
            if len(parts) == 2:
                date_str, target = parts
            else:
                date_str = key
                target = 'ALL'

            try:
                note_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except:
                continue

            todo_list.append({
                'key': key,
                'date': note_date,
                'target': target,
                'content': val.get('content', ''),
                'tag': val.get('tag', '일반'),
                'completed': completed,
                'updated_at': val.get('updated_at', ''),
            })

        # 미래 먼저, 과거는 뒤로
        todo_list.sort(key=lambda x: (x['completed'], x['date'] < TODAY, x['date']))

        if not todo_list:
            st.info("📭 해당 조건의 할 일이 없습니다.")
        else:
            # 통계
            total = len(todo_list)
            done = sum(1 for t in todo_list if t['completed'])
            progress = (done / total * 100) if total > 0 else 0

            col_s1, col_s2, col_s3 = st.columns(3)
            col_s1.metric("전체 할 일", f"{total}개")
            col_s2.metric("완료", f"{done}개")
            col_s3.metric("진행률", f"{progress:.0f}%")

            st.progress(progress / 100)
            st.divider()

            # 할 일 카드
            for todo in todo_list:
                tag_colors = {
                    '일반': ('#E3F2FD', '#1976D2'),
                    '의사결정': ('#FFF3E0', '#F57C00'),
                    '경고': ('#FFEBEE', '#D32F2F'),
                    '대기': ('#F3E5F5', '#7B1FA2'),
                }
                bg, fg = tag_colors.get(todo['tag'], ('#F5F5F5', '#424242'))

                is_past_date = todo['date'] < TODAY
                is_completed = todo['completed']

                opacity = 0.5 if (is_completed or is_past_date) else 1.0
                strikethrough = "text-decoration:line-through;" if is_completed else ""

                col_c1, col_c2 = st.columns([1, 10])
                with col_c1:
                    new_completed = st.checkbox(
                        "완료",
                        value=is_completed,
                        key=f"todo_check_{todo['key']}",
                        label_visibility="collapsed"
                    )
                    if new_completed != is_completed:
                        toggle_note_completed(todo['key'], new_completed)
                        st.rerun()

                with col_c2:
                    date_info = f"{todo['date'].strftime('%Y-%m-%d')} ({WEEKDAYS_KR[todo['date'].weekday()]})"
                    if is_past_date and not is_completed:
                        date_info += " 🕐 지난 날짜"
                    elif not is_past_date:
                        days_until = (todo['date'] - TODAY).days
                        if days_until == 0:
                            date_info += " ⚡ 오늘!"
                        else:
                            date_info += f" · D-{days_until}"

                    st.markdown(f"""
                    <div style='background:{bg}; border-left:5px solid {fg}; 
                                padding:12px 15px; border-radius:6px; margin-bottom:8px;
                                opacity:{opacity};'>
                        <div style='display:flex; justify-content:space-between; margin-bottom:6px;'>
                            <span style='font-weight:bold; color:{fg};'>
                                {date_info} · {todo['target']}
                            </span>
                            <span style='background:{fg}; color:white; padding:2px 8px; 
                                         border-radius:10px; font-size:11px;'>{todo['tag']}</span>
                        </div>
                        <div style='color:#333; font-size:14px; white-space:pre-wrap; {strikethrough}'>{todo['content']}</div>
                    </div>
                    """, unsafe_allow_html=True)

    # =============== TAB 9: 📊 전년 동기 비교 (NEW!) ===============
    with tab9:
        st.subheader("📊 전년 동기 비교")
        st.caption("올해 같은 날짜와 작년 같은 날짜의 평균 가격 비교")

        with st.spinner("작년 데이터 조회 중..."):
            snapshots_400 = load_all_snapshots_history(days_back=400)
            yoy_df = get_year_over_year_comparison(curr, snapshots_400)

        if yoy_df.empty:
            st.info("📭 비교할 데이터가 없습니다.")
        elif yoy_df['작년_가격'].notna().sum() == 0:
            st.info(f"""
            📭 작년 동기 데이터가 쌓이지 않았습니다.
            
            **왜 데이터가 없나요?**
            - 이 앱은 시작한 지 얼마 안 됐기 때문에 작년 같은 날짜의 스냅샷이 없어요
            - **매일 '🚀 오늘 내역 저장' 버튼을 누르면** 1년 후 이 탭에서 의미 있는 비교가 가능해져요
            - 현재는 작년 가격이 표시되지 않지만, 올해 가격은 참고로 확인할 수 있어요
            """)
            # 올해만이라도 표시
            st.subheader("📊 올해 평균 가격")
            display_yoy = yoy_df.copy()
            display_yoy['날짜'] = display_yoy['날짜'].apply(lambda x: x.strftime('%Y-%m-%d'))
            display_yoy['올해_가격'] = display_yoy['올해_가격'].apply(lambda x: f"₩{int(x):,}")
            st.dataframe(display_yoy[['날짜', '요일', '올해_가격']], use_container_width=True, hide_index=True)
        else:
            valid_yoy = yoy_df[yoy_df['작년_가격'].notna()]

            # 요약 통계
            if not valid_yoy.empty:
                avg_curr = valid_yoy['올해_가격'].mean()
                avg_last = valid_yoy['작년_가격'].mean()
                avg_change = (avg_curr - avg_last) / avg_last * 100 if avg_last > 0 else 0

                col_y1, col_y2, col_y3 = st.columns(3)
                col_y1.metric("올해 평균가", f"₩{int(avg_curr):,}")
                col_y2.metric("작년 평균가", f"₩{int(avg_last):,}")
                col_y3.metric("전년 대비", f"{avg_change:+.1f}%",
                              delta=f"₩{int(avg_curr - avg_last):+,}")

                # 트렌드 차트
                fig_yoy = go.Figure()
                valid_yoy_sorted = valid_yoy.sort_values('날짜')
                valid_yoy_sorted['날짜_dt'] = pd.to_datetime(valid_yoy_sorted['날짜'])

                fig_yoy.add_trace(go.Scatter(
                    x=valid_yoy_sorted['날짜_dt'], y=valid_yoy_sorted['올해_가격'],
                    mode='lines+markers', name='올해',
                    line=dict(color='#D32F2F', width=3)
                ))
                fig_yoy.add_trace(go.Scatter(
                    x=valid_yoy_sorted['날짜_dt'], y=valid_yoy_sorted['작년_가격'],
                    mode='lines+markers', name='작년',
                    line=dict(color='#1976D2', width=2, dash='dash')
                ))
                fig_yoy.update_layout(
                    template="plotly_white", height=400,
                    title="올해 vs 작년 평균가 비교",
                    hovermode='x unified'
                )
                fig_yoy.update_yaxes(tickformat=",")
                st.plotly_chart(fig_yoy, use_container_width=True)

            # 상세 테이블
            st.subheader("📋 일별 상세")
            display_yoy = yoy_df.copy()
            display_yoy['날짜'] = display_yoy['날짜'].apply(lambda x: x.strftime('%Y-%m-%d'))
            display_yoy['작년_같은날'] = display_yoy['작년_같은날'].apply(lambda x: x.strftime('%Y-%m-%d') if x else '-')
            display_yoy['올해_가격'] = display_yoy['올해_가격'].apply(lambda x: f"₩{int(x):,}")
            display_yoy['작년_가격'] = display_yoy['작년_가격'].apply(lambda x: f"₩{int(x):,}" if pd.notna(x) else "-")
            display_yoy['차이'] = display_yoy['차이'].apply(lambda x: f"{'+' if x and x > 0 else ''}₩{int(x):,}" if pd.notna(x) else "-")
            display_yoy['증감률(%)'] = display_yoy['증감률(%)'].apply(lambda x: f"{'+' if x and x > 0 else ''}{x:.1f}%" if pd.notna(x) else "-")
            st.dataframe(display_yoy, use_container_width=True, hide_index=True)

    # =============== TAB 10: PDF ===============
    with tab10:
        st.subheader("📄 회장님 보고용 PDF")
        st.caption("⚠️ 한글 PDF를 위해서는 GitHub 레포에 NanumGothic.ttf 파일이 필요합니다.")

        period_mode = st.radio("기간", ["📅 월별", "📆 전체", "🎯 커스텀"], horizontal=True)
        all_dates = sorted(curr['Date'].unique())
        data_min = min(all_dates); data_max = max(all_dates)

        if period_mode == "📅 월별":
            available_months = sorted(set((d.year, d.month) for d in all_dates))
            month_options = [f"{y}년 {m}월" for y, m in available_months]
            if not month_options:
                st.warning("데이터 없음")
                st.stop()
            sel_str = st.selectbox("월", month_options)
            sel_y = int(sel_str.split("년")[0])
            sel_m = int(sel_str.split("년")[1].replace("월", "").strip())
            _, ld = calendar.monthrange(sel_y, sel_m)
            pdf_start = max(date(sel_y, sel_m, 1), data_min)
            pdf_end = min(date(sel_y, sel_m, ld), data_max)
            period_label = f"{sel_y}년 {sel_m}월"
        elif period_mode == "📆 전체":
            pdf_start, pdf_end = data_min, data_max
            period_label = f"{pdf_start} ~ {pdf_end}"
        else:
            c1, c2 = st.columns(2)
            with c1: pdf_start = st.date_input("시작", value=data_min, min_value=data_min, max_value=data_max)
            with c2: pdf_end = st.date_input("종료", value=data_max, min_value=data_min, max_value=data_max)
            period_label = f"{pdf_start} ~ {pdf_end}"

        filtered = curr[(curr['Date'] >= pdf_start) & (curr['Date'] <= pdf_end)]
        if filtered.empty:
            st.warning("해당 기간 데이터 없음")
        else:
            preview_opp = calculate_opportunity_cost(
                filtered, df_flight_all, df_comp_all,
                josun_threshold, flight_threshold,
                active_search_date, events=all_events, sensitivity=sensitivity
            )
            if not preview_opp.empty:
                prev_positive = preview_opp[preview_opp['기회비용'] > 0]['기회비용'].sum()
                prev_boosted = (preview_opp['BAR상승'] > 0).sum()
                c1, c2, c3 = st.columns(3)
                c1.metric("기간", period_label)
                c2.metric("추가 수익", f"₩{int(prev_positive):,}")
                c3.metric("시그널", f"{prev_boosted}건")

                # 한글 폰트 체크
                if os.path.exists("NanumGothic.ttf"):
                    st.success("✅ 한글 폰트 감지됨 - 한글 PDF 생성 가능")
                else:
                    st.warning("⚠️ NanumGothic.ttf 파일이 없습니다. 영문 PDF로 생성됩니다. (GitHub에 폰트 파일 추가 필요)")

                if st.button("📄 PDF 생성", type="primary", use_container_width=True):
                    with st.spinner("생성 중..."):
                        try:
                            # 스냅샷 로드 (정확도 검증용)
                            snapshots_for_pdf = load_all_snapshots_history(days_back=90)
                            pdf_bytes = generate_pdf_report(
                                preview_opp, period_label,
                                josun_threshold, flight_threshold,
                                pdf_start.strftime('%Y-%m-%d'),
                                pdf_end.strftime('%Y-%m-%d'),
                                curr_df=filtered,
                                df_flight=df_flight_all,
                                df_comp=df_comp_all,
                                events=all_events,
                                sensitivity=sensitivity,
                                active_search_date=active_search_date,
                                snapshots_list=snapshots_for_pdf
                            )
                            file_label = period_label.replace(" ", "_").replace("년", "").replace("월", "")
                            st.download_button("📥 다운로드", data=pdf_bytes,
                                file_name=f"Amber_Report_{file_label}.pdf",
                                mime="application/pdf", use_container_width=True)
                            st.success("✅ 생성 완료!")
                        except Exception as e:
                            st.error(f"실패: {e}")

# =============== TAB 11: 💡 종합 인사이트 (경영진 보고용) ===============
    with tab11:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #1E3C72 0%, #2A5298 100%); 
                    padding: 25px; border-radius: 12px; color: white; margin-bottom: 20px;'>
            <h2 style='margin-top:0; color:#FFD700;'>💡 종합 수익 진단 리포트</h2>
            <div style='font-size: 14px; opacity: 0.9;'>시스템 권장 vs 실제 의사결정을 종합하여 현재의 수익 관리 건전성을 진단합니다.</div>
        </div>
        """, unsafe_allow_html=True)

        if curr_ui.empty:
            st.warning("데이터가 없습니다.")
        else:
            future_dates = sorted([d for d in curr_ui['Date'].unique() if d >= TODAY])
            
            # --- 1. 데이터 집계 (의사결정 성적표) ---
            decision_counts = {'✅ 전략 일치': 0, '⭐ 수동 개입': 0, '⚠️ 수익 포기': 0, '🤖 변동 없음': 0}
            
            for d in future_dates:
                date_str = d.strftime('%Y-%m-%d')
                type_code, season, is_weekend = get_season_details(d)
                
                for rid in DYNAMIC_ROOMS:
                    curr_match = curr_ui[(curr_ui['RoomID'] == rid) & (curr_ui['Date'] == d)]
                    if curr_match.empty: continue
                    
                    avail = curr_match.iloc[0]['Available']
                    total = curr_match.iloc[0]['Total']
                    occ = ((total - avail) / total * 100) if total > 0 else 0
                    
                    pure_sys_bar = determine_bar(season, is_weekend, occ)
                    
                    prev_bar = None
                    if not prev_ui.empty:
                        prev_m = prev_ui[(prev_ui['RoomID'] == rid) & (prev_ui['Date'] == d)]
                        if not prev_m.empty:
                            _, prev_bar, _, _ = get_final_values(rid, d, prev_m.iloc[0]['Available'], prev_m.iloc[0]['Total'])
                    
                    current_cms_bar = prev_bar if prev_bar else pure_sys_bar
                    applied_bar = applied_rates_data.get(date_str, {}).get('rooms', {}).get(rid) if applied_rates_data else None
                    
                    if applied_bar:
                        if applied_bar == pure_sys_bar: decision_counts['✅ 전략 일치'] += 1
                        else: decision_counts['⭐ 수동 개입'] += 1
                    else:
                        if pure_sys_bar != current_cms_bar: decision_counts['⚠️ 수익 포기'] += 1
                        else: decision_counts['🤖 변동 없음'] += 1

            # --- 2. 최상단 핵심 KPI ---
            total_decisions = sum(decision_counts.values())
            loss_rate = (decision_counts['⚠️ 수익 포기'] / total_decisions * 100) if total_decisions > 0 else 0
            future_opp = alert_opp_df[alert_opp_df['날짜'] >= TODAY]['기회비용'].sum() if not alert_opp_df.empty else 0
            
            col_k1, col_k2, col_k3 = st.columns(3)
            with col_k1:
                st.markdown(f"""
                <div style='background:#FFF; border-left:5px solid #D32F2F; padding:20px; border-radius:8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);'>
                    <div style='color:#666; font-size:14px; font-weight:bold;'>누수 중인 수익 (미래)</div>
                    <div style='color:#D32F2F; font-size:28px; font-weight:bold; margin-top:5px;'>₩ {int(future_opp):,}</div>
                </div>
                """, unsafe_allow_html=True)
            with col_k2:
                st.markdown(f"""
                <div style='background:#FFF; border-left:5px solid #FF8F00; padding:20px; border-radius:8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);'>
                    <div style='color:#666; font-size:14px; font-weight:bold;'>수익 포기(미조치) 비율</div>
                    <div style='color:#FF8F00; font-size:28px; font-weight:bold; margin-top:5px;'>{loss_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            with col_k3:
                st.markdown(f"""
                <div style='background:#FFF; border-left:5px solid #2E7D32; padding:20px; border-radius:8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);'>
                    <div style='color:#666; font-size:14px; font-weight:bold;'>수동 개입/전략 일치</div>
                    <div style='color:#2E7D32; font-size:28px; font-weight:bold; margin-top:5px;'>{decision_counts['⭐ 수동 개입']}건 / {decision_counts['✅ 전략 일치']}건</div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # --- 3. 시각화 대시보드 ---
            col_c1, col_c2 = st.columns(2)
            
            # 도넛 차트: 의사결정 상태
            with col_c1:
                st.subheader("🎯 의사결정 포트폴리오 (미래)")
                df_pie = pd.DataFrame(list(decision_counts.items()), columns=['상태', '건수'])
                df_pie = df_pie[df_pie['건수'] > 0]
                
                color_map = {
                    '✅ 전략 일치': '#4CAF50', '⭐ 수동 개입': '#FF9800',
                    '⚠️ 수익 포기': '#F44336', '🤖 변동 없음': '#E0E0E0'
                }
                
                fig_pie = px.pie(df_pie, values='건수', names='상태', hole=0.4,
                                 color='상태', color_discrete_map=color_map)
                fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                fig_pie.update_layout(showlegend=False, height=350, margin=dict(t=30, b=0, l=0, r=0))
                st.plotly_chart(fig_pie, use_container_width=True)

            # 바 차트: 기회비용 취약점 (어디서 돈이 새고 있나?)
            with col_c2:
                st.subheader("🚨 객실별 수익 누수 현황")
                if not alert_opp_df.empty:
                    future_opp_df = alert_opp_df[alert_opp_df['날짜'] >= TODAY]
                    room_loss = future_opp_df.groupby('객실타입')['기회비용'].sum().reset_index()
                    room_loss = room_loss[room_loss['기회비용'] > 0].sort_values('기회비용', ascending=True)
                    
                    if not room_loss.empty:
                        fig_bar = px.bar(room_loss, x='기회비용', y='객실타입', orientation='h',
                                         text_auto='.2s', color='기회비용', color_continuous_scale='Reds')
                        fig_bar.update_layout(showlegend=False, height=350, margin=dict(t=30, b=0, l=0, r=0),
                                              xaxis_title="누수액 (KRW)", yaxis_title="")
                        st.plotly_chart(fig_bar, use_container_width=True)
                    else:
                        st.info("현재 새어나가는 수익(기회비용)이 없습니다. 완벽합니다!")
                else:
                    st.info("시뮬레이터 데이터가 없습니다.")

            st.divider()

            # --- 4. 자동 생성 결론 브리핑 ---
            st.subheader("📝 AI 종합 진단 결론")
            
            diagnosis_msg = ""
            if loss_rate > 30:
                diagnosis_msg = f"<span style='color:#D32F2F; font-weight:bold;'>🔴 위험:</span> 전체 타겟 일정의 {loss_rate:.1f}%가 시스템이 인상을 권고했음에도 방치되어 <b>'수익 포기(⚠️)'</b> 상태에 있습니다. 점유율 채우기에 매몰되어 ADR 상향 기회를 잃고 있으며, 즉각적인 요금 오버라이드 조치가 시급합니다."
            elif loss_rate > 10:
                diagnosis_msg = f"<span style='color:#FF8F00; font-weight:bold;'>🟡 주의:</span> 일정 부분({loss_rate:.1f}%)에서 시장 강세를 반영하지 못한 '수익 포기'가 발생하고 있습니다. 의도적인 홀딩이 아니라면 놓친 날짜들을 점검하여 기회비용(₩{int(future_opp):,})을 회수해야 합니다."
            else:
                diagnosis_msg = f"<span style='color:#2E7D32; font-weight:bold;'>🟢 우수:</span> 수익 포기 비율이 {loss_rate:.1f}%로 매우 낮으며, 시장 시그널과 RM 의사결정이 긴밀하게 동기화되어 수익을 극대화하고 있습니다."

            st.markdown(f"""
            <div style='background:#F8F9FA; border-left:4px solid #1E3C72; padding:20px; font-size:15px; line-height:1.6;'>
                <b>1. 현재 상황 요약:</b><br>
                미래 일정에 대해 총 <b>{total_decisions}건</b>의 가격 판단이 이루어지고 있으며, 이 중 우리가 시장 논리에 맞춰 선제적으로 개입(전략 일치 및 수동 개입)한 비율은 <b>{((decision_counts['✅ 전략 일치'] + decision_counts['⭐ 수동 개입']) / total_decisions * 100) if total_decisions > 0 else 0:.1f}%</b>입니다.<br><br>
                <b>2. 진단 결과:</b><br>
                {diagnosis_msg}
            </div>
            """, unsafe_allow_html=True)

else:
    st.info("👈 사이드바에서 잔여객실 파일을 업로드하세요.")
    st.markdown(f"""
    ### 🎯 v8.0 새 기능

    - 🕐 **과거 날짜 알림 제외** (오늘 {TODAY.strftime('%Y-%m-%d')} 이후만)
    - 📄 **PDF 한글 폰트 지원** (NanumGothic.ttf 필요)
    - 📅 **주간 요약 리포트** (지난주/이번주/다음주)
    - 🎯 **할 일 체크리스트** (의사결정 태그)
    - 📊 **전년 동기 비교** (작년 같은 날 vs 올해)
    """)

            
                                    

