"""
🔧 APP3 (Secret Strategy Lab) - Firebase 완전 이전판
Supabase 제거 + Firebase로 모든 백업/복원 기능 이전
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from datetime import datetime, timedelta, timezone 
import calendar
import re
import os
import base64
import json
from io import BytesIO
from fpdf import FPDF
import firebase_admin
from firebase_admin import credentials, firestore


# ============================================================
# 🔥 Firebase 초기화 (하이브리드 DB 체제)
# ============================================================
# 1) 기본 앱 (viva_key.json / st.secrets["firebase"])
if not firebase_admin._apps:
    try:
        cred_dict = dict(st.secrets["firebase"])
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
    except Exception:
        pass

# 2) 호텔 DB 앱 (Command Center와 공유)
try:
    app_hotel = firebase_admin.get_app("hotel_app")
except ValueError:
    try:
        if "firebase_hotel" in st.secrets:
            cred_h = credentials.Certificate(dict(st.secrets["firebase_hotel"]))
        elif "firebase" in st.secrets:
            cred_h = credentials.Certificate(dict(st.secrets["firebase"]))
        else:
            raise ValueError("No hotel credentials")
        app_hotel = firebase_admin.initialize_app(cred_h, name="hotel_app")
    except Exception:
        app_hotel = None

try:
    db_hotel = firestore.client(app=app_hotel) if app_hotel else None
except Exception:
    db_hotel = None

# 3) 항공/전략 DB 앱 (App3 스냅샷 저장소)
try:
    app_flight = firebase_admin.get_app("flight_app")
except ValueError:
    try:
        if "firebase_flight" in st.secrets:
            secret_dict = dict(st.secrets["firebase_flight"])
            if "private_key" in secret_dict:
                secret_dict["private_key"] = secret_dict["private_key"].replace("\\n", "\n")
            cred_f = credentials.Certificate(secret_dict)
            app_flight = firebase_admin.initialize_app(cred_f, name="flight_app")
        else:
            app_flight = None
    except Exception:
        app_flight = None

try:
    db_flight = firestore.client(app=app_flight) if app_flight else None
except Exception:
    db_flight = None


# ============================================================
# 🔥 Firebase 기반 백업/복원 함수들 (Supabase 완전 대체)
# ============================================================
# 저장소: db_flight의 'amber_snapshots' 컬렉션
# 문서 ID: snapshot_{KST타임스탬프}
# 문서 구조:
#   - save_name: 백업 이름
#   - timestamp: 정수 타임스탬프 (정렬/쿼리용)
#   - created_at: KST 일시 문자열
#   - pms_data: 압축된 PMS JSON 문자열 (있을 때만)
#   - sob_data: SOB Map
#   - avail_data: Avail List
#   - is_compressed: True (PMS 압축 여부)

SNAPSHOT_COLLECTION = "amber_snapshots"
MAX_SNAPSHOTS = 20  # 자동 정리 임계치


def datetime_handler(x):
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.isoformat()
    raise TypeError(f"Object of type {type(x)} is not JSON serializable")


def save_to_cloud(save_name, pms_df, sob_data, avail_data):
    """
    🔥 Firebase 버전 백업 저장
    - PMS는 일별/타입별 집계로 압축 (용량 90% 절감)
    - 저장 시 자동으로 오래된 백업 정리 (MAX_SNAPSHOTS 초과분)
    - 문서 ID는 KST 타임스탬프 기반
    """
    if not db_flight:
        st.sidebar.error("❌ Firebase 연결 실패: 저장 불가")
        return
    
    try:
        # --------------------------------------------------------
        # 1. PMS 데이터 압축 - 일별 집계만 저장
        # --------------------------------------------------------
        compressed_pms = None
        if not pms_df.empty:
            if all(c in pms_df.columns for c in ['Stay_Date', '객실타입', 'Daily_Rev', 'Daily_RN']):
                agg_dict = {'Daily_Rev': 'sum', 'Daily_RN': 'sum'}
                if 'Temp_Bk' in pms_df.columns:
                    agg_dict['Temp_Bk'] = 'min'
                compressed = pms_df.groupby(['Stay_Date', '객실타입']).agg(agg_dict).reset_index()
                compressed_pms = compressed.to_json(orient='split', date_format='iso')
            else:
                compressed_pms = pms_df.to_json(orient='split', date_format='iso')

        # --------------------------------------------------------
        # 2. 저장 전에 오래된 백업 자동 삭제
        # --------------------------------------------------------
        try:
            all_docs = list(
                db_flight.collection(SNAPSHOT_COLLECTION)
                .order_by("timestamp", direction=firestore.Query.DESCENDING)
                .stream()
            )
            if len(all_docs) >= MAX_SNAPSHOTS:
                # 최신 MAX_SNAPSHOTS-1개만 남기고 삭제
                docs_to_delete = all_docs[MAX_SNAPSHOTS-1:]
                for old_doc in docs_to_delete:
                    old_doc.reference.delete()
                if docs_to_delete:
                    st.sidebar.info(f"🗑️ 오래된 백업 {len(docs_to_delete)}개 자동 정리")
        except Exception as e:
            st.sidebar.warning(f"자동 정리 실패 (계속 진행): {e}")

        # --------------------------------------------------------
        # 3. 새 백업 저장
        # --------------------------------------------------------
        kst_now = datetime.now(timezone(timedelta(hours=9)))
        unique_ts = int(kst_now.timestamp())
        doc_id = f"snapshot_{unique_ts}"
        
        payload = {
            "save_name": save_name,
            "timestamp": unique_ts,
            "created_at": kst_now.strftime('%Y-%m-%d %H:%M:%S'),
            "sob_data": json.loads(json.dumps(sob_data, default=datetime_handler)) if sob_data else {},
            "avail_data": json.loads(json.dumps(avail_data, default=datetime_handler)) if avail_data else [],
            "is_compressed": True
        }
        
        # PMS는 문자열로 저장 (JSON 안 깊이 제한 회피)
        if compressed_pms:
            # Firestore 1MB 제한 체크
            pms_size = len(compressed_pms.encode('utf-8'))
            if pms_size > 900_000:  # 900KB 넘으면 경고
                st.sidebar.warning(f"⚠️ PMS 데이터가 큽니다 ({pms_size/1024:.0f}KB). 추가 압축 필요.")
                # 날짜 범위 더 좁게 재집계 (타입만 남기고 Stay_Date 축약)
                try:
                    temp_df = pd.read_json(compressed_pms, orient='split')
                    if 'Stay_Date' in temp_df.columns:
                        temp_df['Stay_Date'] = pd.to_datetime(temp_df['Stay_Date']).dt.strftime('%Y-%m-%d')
                    compressed_pms = temp_df.to_json(orient='split')
                except:
                    pass
            payload["pms_data"] = compressed_pms
        else:
            payload["pms_data"] = None

        db_flight.collection(SNAPSHOT_COLLECTION).document(doc_id).set(payload)
        
        st.sidebar.success(f"✅ [{save_name}] Firebase 백업 완료! (압축 적용)")
        
    except Exception as e:
        st.sidebar.error(f"❌ 저장 실패: {e}")


def get_snapshots_by_date(selected_date):
    """
    🔥 Firebase 버전: 선택한 날짜 범위의 백업 리스트 조회
    """
    if not db_flight:
        return []
    
    try:
        start_dt = datetime.combine(selected_date, datetime.min.time())
        end_dt = datetime.combine(selected_date, datetime.max.time())
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        
        docs = (
            db_flight.collection(SNAPSHOT_COLLECTION)
            .where("timestamp", ">=", start_ts)
            .where("timestamp", "<=", end_ts)
            .order_by("timestamp", direction=firestore.Query.DESCENDING)
            .stream()
        )
        
        snaps = []
        for doc in docs:
            try:
                d = doc.to_dict()
                name = d.get("save_name", "이름 없는 백업")
                ts = d.get("timestamp", 0)
                dt_obj = datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=9)))
                time_str = dt_obj.strftime('%H:%M')
                snaps.append({"id": doc.id, "name": f"[{time_str}] {name}"})
            except Exception:
                pass
        return snaps
    except Exception as e:
        st.sidebar.error(f"데이터 조회 중 오류: {e}")
        return []


def load_snapshot_data(snap_id):
    """
    🔥 Firebase 버전: 특정 스냅샷 불러오기
    """
    if not db_flight:
        return pd.DataFrame(), {}, []
    
    try:
        doc = db_flight.collection(SNAPSHOT_COLLECTION).document(snap_id).get()
        if not doc.exists:
            return pd.DataFrame(), {}, []
        
        d = doc.to_dict()
        
        # PMS 복원
        pms_df = pd.DataFrame()
        pms_raw = d.get('pms_data')
        if pms_raw:
            try:
                import io
                pms_df = pd.read_json(io.StringIO(pms_raw), orient='split')
            except Exception as e:
                st.warning(f"PMS 복원 경고: {e}")
        
        sob_data = d.get('sob_data') or {}
        avail_data = d.get('avail_data') or []
        
        # 압축된 포맷 알림
        if d.get('is_compressed'):
            st.sidebar.info("📦 압축된 백업을 불러왔습니다 (일별 집계본)")
        
        return pms_df, sob_data, avail_data
        
    except Exception as e:
        st.error(f"데이터 로드 에러: {e}")
    
    return pd.DataFrame(), {}, []


def delete_snapshot(snap_id):
    """
    🔥 Firebase 버전: 특정 백업 삭제
    """
    if not db_flight:
        return False
    try:
        db_flight.collection(SNAPSHOT_COLLECTION).document(snap_id).delete()
        return True
    except Exception as e:
        st.error(f"삭제 실패: {e}")
        return False


def emergency_cleanup_firebase(keep_recent=10):
    """
    🚨 긴급: Firebase에서 최근 N개만 남기고 전부 삭제
    """
    if not db_flight:
        return 0
    try:
        all_docs = list(
            db_flight.collection(SNAPSHOT_COLLECTION)
            .order_by("timestamp", direction=firestore.Query.DESCENDING)
            .stream()
        )
        if len(all_docs) <= keep_recent:
            return 0
        
        docs_to_delete = all_docs[keep_recent:]
        deleted_count = 0
        for old_doc in docs_to_delete:
            try:
                old_doc.reference.delete()
                deleted_count += 1
            except Exception:
                pass
        return deleted_count
    except Exception as e:
        st.error(f"긴급 정리 실패: {e}")
        return 0


# ============================================================
# 📄 PDF 리포트 (기존과 동일)
# ============================================================
def export_comprehensive_report(data):
    pdf = FPDF()
    pdf.set_margins(left=20, top=20, right=20) 
    pdf.add_page()
    
    pdf.set_fill_color(26, 42, 68) 
    pdf.rect(0, 0, 210, 297, 'F')
    pdf.set_fill_color(166, 138, 86) 
    pdf.rect(0, 100, 210, 5, 'F')
    
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("helvetica", "B", 35)
    pdf.set_xy(20, 120)
    pdf.cell(0, 20, "STRATEGIC REVENUE", ln=True)
    pdf.cell(0, 20, "ANALYSIS REPORT", ln=True)
    
    pdf.set_font("helvetica", "", 12)
    pdf.ln(80)
    pdf.cell(0, 10, f"TARGET PERIOD: 2026 / {data['month']}nd", ln=True, align='R')
    pdf.cell(0, 10, f"PREPARED BY: S&M ARCHITECT JEON", ln=True, align='R')
    pdf.cell(0, 10, f"REPORT DATE: {data['date']}", ln=True, align='R')

    pdf.add_page()
    pdf.set_text_color(26, 42, 68)
    pdf.set_font("helvetica", "B", 20)
    pdf.cell(0, 15, "01. KEY PERFORMANCE INDICATORS", ln=True)
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(20, 35, 30, 2, 'F')
    pdf.ln(15)
    
    pdf.set_font("helvetica", "B", 12)
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(60, 12, "METRIC", 1, 0, 'C', True)
    pdf.cell(55, 12, "ACTUAL", 1, 0, 'C', True)
    pdf.cell(55, 12, "ACHIEVEMENT", 1, 1, 'C', True)
    
    pdf.set_font("helvetica", "", 11)
    pdf.cell(60, 12, "Gross Revenue", 1)
    pdf.cell(55, 12, f"KRW {data['act_rev']:,.0f}", 1, 0, 'C')
    pdf.cell(55, 12, f"{data['rev_pct']:.1f}%", 1, 1, 'C')
    
    pdf.cell(60, 12, "Average ADR", 1)
    pdf.cell(55, 12, f"KRW {data['act_adr']:,.0f}", 1, 0, 'C')
    pdf.cell(55, 12, f"{data['adr_diff']:+,}", 1, 1, 'C')

    pdf.ln(15)

    pdf.set_font("helvetica", "B", 14)
    pdf.cell(0, 10, "Performance vs Target Visualization", ln=True)
    
    pdf.set_font("helvetica", "", 10)
    pdf.cell(0, 8, "Revenue Achievement Rate:", ln=True)
    pdf.set_fill_color(230, 230, 230)
    pdf.rect(20, pdf.get_y(), 170, 8, 'F')
    bar_width = min(170, (data['rev_pct'] / 100) * 170)
    pdf.set_fill_color(26, 42, 68) if data['rev_pct'] >= 100 else pdf.set_fill_color(158, 42, 43)
    pdf.rect(20, pdf.get_y(), bar_width, 8, 'F')
    pdf.ln(12)

    pdf.cell(0, 8, "Room Nights Achievement Rate:", ln=True)
    pdf.set_fill_color(230, 230, 230)
    pdf.rect(20, pdf.get_y(), 170, 8, 'F')
    bar_width_rn = min(170, (data['rn_pct'] / 100) * 170)
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(20, pdf.get_y(), bar_width_rn, 8, 'F')
    pdf.ln(20)

    pdf.add_page()
    pdf.set_font("helvetica", "B", 20)
    pdf.cell(0, 15, "02. STRATEGIC INSIGHTS", ln=True)
    pdf.set_fill_color(166, 138, 86)
    pdf.rect(20, 35, 30, 2, 'F')
    pdf.ln(10)
    
    pdf.set_font("helvetica", "B", 14)
    pdf.set_text_color(166, 138, 86)
    pdf.cell(0, 10, "Yielding Profitability Analysis", ln=True)
    
    pdf.set_fill_color(248, 245, 240)
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("helvetica", "", 12)
    
    insight_msg = (
        f"By applying the 'Architect High-Tier Strategy' (ADR +{data['adj_adr']}%), "
        f"it is analyzed that a net gain of KRW {data['gain']:,} could be realized. "
        "This indicates a significant opportunity to shift from volume-driven "
        "to value-driven sales, reducing variable costs by up to 15%."
    )
    pdf.multi_cell(w=pdf.epw, h=10, txt=insight_msg, border=1, fill=True)
    
    pdf.ln(20)
    pdf.set_font("helvetica", "B", 14)
    pdf.cell(0, 10, "Market & Action Plan", ln=True)
    pdf.set_font("helvetica", "", 11)
    
    plans = [
        f"- Target ADR: Maintenance of KRW {data['act_adr']*1.1:,.0f} for next weekend peak.",
        "- Channel Mix: Reducing OTA dependency to increase Net RevPAR.",
        "- Yielding: Immediate Tier-Up (+1) recommended for upcoming sold-out dates."
    ]
    for plan in plans:
        pdf.cell(0, 8, plan, ln=True)

    pdf.set_y(275)
    pdf.set_font("helvetica", "I", 8)
    pdf.set_text_color(150, 150, 150)
    pdf.cell(0, 10, "CONFIDENTIAL | AMBER PURE HILL STRATEGY", 0, 0, 'C')

    return bytes(pdf.output())


# ============================================================
# 🔧 유틸 함수들 (기존 그대로 유지)
# ============================================================
def clean_numeric(val):
    if val is None: return 0.0
    if isinstance(val, pd.Series): val = val.iloc[-1] 
    if pd.isna(val): return 0.0
    try:
        s_val = str(val).replace(',', '').replace('%', '').replace('₩', '').strip()
        if s_val.lower() in ['', '-', 'nan', 'none', 'null']: return 0.0
        return float(s_val)
    except:
        return 0.0

def deduplicate_columns(cols):
    new_cols = []; seen = {}
    for c in cols:
        c_str = str(c).strip()
        if c_str in seen:
            seen[c_str] += 1
            new_cols.append(f"{c_str}_{seen[c_str]}")
        else:
            seen[c_str] = 0
            new_cols.append(c_str)
    return new_cols

def find_column(df, keywords):
    if df is None or df.empty: return None
    matched_cols = []
    for col in df.columns:
        clean_col = str(col).replace(' ', '').replace('\n', '').replace('\r', '')
        for kw in keywords:
            if kw in clean_col:
                matched_cols.append(col)
    return matched_cols[-1] if matched_cols else None

def extract_date_from_avail(df, file_name):
    try:
        top_text = df.iloc[:5].astype(str).apply(lambda x: ' '.join(x), axis=1).str.cat(sep=' ')
        match = re.search(r'시작일자\s*:\s*(\d{4}-\d{2}-\d{2})', top_text)
        if match: return datetime.strptime(match.group(1), '%Y-%m-%d')
    except: pass
    
    name_match = re.search(r'(\d{8})', str(file_name))
    if name_match:
        try: return datetime.strptime(name_match.group(1), '%Y%m%d')
        except: pass
    return datetime.now()


# ==========================================
# 🌟 글로벌 변수 및 시즌/티어 정밀 룰 세팅
# ==========================================
TARGET_DATA = {
    1:  {"rn": 2270, "adr": 226869, "occ": 56.3, "rev": 514992575},
    2:  {"rn": 2577, "adr": 305227, "occ": 70.8, "rev": 786570856},
    3:  {"rn": 2248, "adr": 235587, "occ": 55.8, "rev": 529599040},
    4:  {"rn": 2414, "adr": 288049, "occ": 61.9, "rev": 695351004},
    5:  {"rn": 3082, "adr": 293220, "occ": 76.5, "rev": 903705440},
    6:  {"rn": 2776, "adr": 291140, "occ": 71.2, "rev": 808203820},
    7:  {"rn": 3671, "adr": 335590, "occ": 91.1, "rev": 1231949142},
    8:  {"rn": 3873, "adr": 358476, "occ": 96.1, "rev": 1388376999},
    9:  {"rn": 2932, "adr": 324752, "occ": 75.2, "rev": 952171506},
    10: {"rn": 3009, "adr": 298163, "occ": 74.7, "rev": 897171539},
    11: {"rn": 2402, "adr": 277746, "occ": 61.6, "rev": 667146771},
    12: {"rn": 2765, "adr": 290788, "occ": 68.6, "rev": 804030110}
}
BUDGET_DATA = {m: TARGET_DATA[m]["rev"] for m in range(1, 13)}
TOTAL_ROOM_CAPACITY = 131

WEEKDAYS_KR = ['월', '화', '수', '목', '금', '토', '일']
DYNAMIC_ROOMS = ["FDB", "FDE", "HDP", "HDT", "HDF"]
FIXED_ROOMS = ["GDB", "GDF", "FFD", "FPT", "PPV"]
ALL_ROOMS = DYNAMIC_ROOMS + FIXED_ROOMS

PRICE_TABLE = {
    "FDB": {"BAR0": 802000, "BAR8": 315000, "BAR7": 353000, "BAR6": 396000, "BAR5": 445000, "BAR4": 502000, "BAR3": 567000, "BAR2": 642000, "BAR1": 728000},
    "FDE": {"BAR0": 839000, "BAR8": 352000, "BAR7": 390000, "BAR6": 433000, "BAR5": 482000, "BAR4": 539000, "BAR3": 604000, "BAR2": 679000, "BAR1": 765000},
    "HDP": {"BAR0": 759000, "BAR8": 280000, "BAR7": 318000, "BAR6": 361000, "BAR5": 410000, "BAR4": 467000, "BAR3": 532000, "BAR2": 607000, "BAR1": 693000},
    "HDT": {"BAR0": 729000, "BAR8": 250000, "BAR7": 288000, "BAR6": 331000, "BAR5": 380000, "BAR4": 437000, "BAR3": 502000, "BAR2": 577000, "BAR1": 663000},
    "HDF": {"BAR0": 916000, "BAR8": 420000, "BAR7": 458000, "BAR6": 501000, "BAR5": 550000, "BAR4": 607000, "BAR3": 672000, "BAR2": 747000, "BAR1": 833000},
}

FIXED_PRICE_TABLE = {
    "GDB": {"UND1": 298000, "UND2": 298000, "MID1": 298000, "MID2": 298000, "UPP1": 298000, "UPP2": 298000, "UPP3":298000},
    "GDF": {"UND1": 375000, "UND2": 410000, "MID1": 410000, "MID2": 488000, "UPP1": 488000, "UPP2": 578000, "UPP3":678000},
    "FFD": {"UND1": 353000, "UND2": 393000, "MID1": 433000, "MID2": 482000, "UPP1": 539000, "UPP2": 604000, "UPP3":704000},
    "FPT": {"UND1": 500000, "UND2": 550000, "MID1": 600000, "MID2": 650000, "UPP1": 700000, "UPP2": 750000, "UPP3":850000},
    "PPV": {"UND1": 1104000, "UND2": 1154000, "MID1": 1154000, "MID2": 1304000, "UPP1": 1304000, "UPP2": 1554000, "UPP3":1704000},
}

FIXED_BAR0_TABLE = {"GDB": 298000, "GDF": 678000, "FFD": 704000, "FPT": 850000, "PPV": 1704000}


def get_season_details(date_obj):
    if isinstance(date_obj, str):
        try: date_obj = datetime.strptime(date_obj[:10], '%Y-%m-%d')
        except: date_obj = datetime.now()
        
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

def get_final_values(room_id, date_obj, avail, total, manual_bar=None):
    type_code, season, is_weekend = get_season_details(date_obj)
    try: current_avail = float(avail) if pd.notna(avail) else 0.0
    except: current_avail = 0.0
    occ = ((total - current_avail) / total * 100) if total > 0 else 0
    
    if manual_bar:
        bar = manual_bar
        if bar == "BAR0":
            if room_id in DYNAMIC_ROOMS: price = PRICE_TABLE.get(room_id, {}).get("BAR0", 0)
            else: price = FIXED_BAR0_TABLE.get(room_id, 0)
        else:
            if room_id in DYNAMIC_ROOMS: price = PRICE_TABLE.get(room_id, {}).get(bar, 0)
            else: price = FIXED_PRICE_TABLE.get(room_id, {}).get(bar, 0)
        return occ, bar, price, True 

    if room_id in DYNAMIC_ROOMS:
        bar = determine_bar(season, is_weekend, occ)
        price = PRICE_TABLE.get(room_id, {}).get(bar, 0)
    else:
        bar = type_code
        price = FIXED_PRICE_TABLE.get(room_id, {}).get(type_code, 0)
    return occ, bar, price, False 

def get_dynamic_bar_tier(occ, date_str):
    type_code, season, is_weekend = get_season_details(date_str)
    return determine_bar(season, is_weekend, occ)

def robust_read_all_sheets(file):
    dfs = []
    try:
        if file.name.endswith('.csv'):
            try: dfs.append(pd.read_csv(file, encoding='cp949', header=None))
            except: dfs.append(pd.read_csv(file, encoding='utf-8-sig', header=None))
        else:
            engine = 'xlrd' if file.name.endswith('.xls') else 'openpyxl'
            xls = pd.ExcelFile(file, engine=engine)
            for sn in xls.sheet_names:
                dfs.append(pd.read_excel(xls, sheet_name=sn, header=None))
    except Exception as e:
        st.sidebar.error(f"❌ '{file.name}' 로드 실패: {e}")
    return dfs

def get_smart_corridor(total_goal, dates, demand_index):
    day_weights = [1.0, 1.0, 1.0, 1.0, 1.8, 2.2, 1.2]
    adj_weights = [day_weights[d.weekday()] * demand_index for d in dates]
    total_w = sum(adj_weights)
    if total_w == 0: return np.zeros(len(dates)), np.zeros(len(dates)), np.zeros(len(dates))
    base = (np.cumsum(adj_weights) / total_w) * total_goal
    return base, base * 1.05, base * 0.95

def get_booking_curve(total_goal, lead_days, demand_idx):
    days = np.arange(-lead_days, 1)
    z = (days + (30 / demand_idx)) / 15
    s_curve = 1 / (1 + np.exp(-z))
    s_curve = (s_curve - s_curve.min()) / (s_curve.max() - s_curve.min())
    return days, s_curve * total_goal


# ==========================================
# 🌟 세션 및 초기 변수 세팅
# ==========================================
if 'oracle_loaded_snap' not in st.session_state:
    st.session_state['oracle_loaded_snap'] = None

if 'oracle_file_key' not in st.session_state:
    st.session_state['oracle_file_key'] = 0

yearly_data_store = {m: {"rev": 0.0, "occ": 0.0, "rn": 0.0, "adr": 0.0} for m in range(1, 13)}
df_full_pms = pd.DataFrame()
real_room_df = None
real_channel_df = None
actual_pace = []
actual_curve = []
avail_analysis = []

# ==========================================
# 사이드바 (상단)
# ==========================================
st.sidebar.title("🧬 Oracle Intelligence v5.5")
selected_month = st.sidebar.selectbox("🎯 분석 타겟 월 선택", range(1, 13), index=3)
demand_idx = st.sidebar.slider("시장 수요 지수 보정", 0.5, 2.0, 1.3)

st.sidebar.markdown("---")
st.sidebar.subheader("📂 전략 데이터 업로드 센터")

pms_files = st.sidebar.file_uploader("PMS 상세 리스트 (다중)", type=['csv', 'xlsx', 'xls'], accept_multiple_files=True, key=f"pms_{st.session_state['oracle_file_key']}")
sob_files = st.sidebar.file_uploader("영업 현황 SOB (다중)", type=['csv', 'xlsx', 'xls'], accept_multiple_files=True, key=f"sob_{st.session_state['oracle_file_key']}")
avail_files = st.sidebar.file_uploader("사용 가능 객실 현황 (다중)", type=['csv', 'xlsx', 'xls'], accept_multiple_files=True, key=f"avail_{st.session_state['oracle_file_key']}")

# ==========================================
# 데이터 파싱 (업로드 vs 클라우드 스냅샷 우선순위 병합)
# ==========================================
if st.session_state['oracle_loaded_snap'] is not None:
    st.sidebar.success("☁️ 클라우드 타임머신 모드 작동 중! (새 파일을 올리면 최신 데이터로 덮어씁니다)")
    df_full_pms = st.session_state['oracle_loaded_snap']['pms'].copy() if not st.session_state['oracle_loaded_snap']['pms'].empty else pd.DataFrame()
    cloud_sob = st.session_state['oracle_loaded_snap']['sob']
    for k, v in cloud_sob.items():
        if str(k).isdigit():
            yearly_data_store[int(k)] = v
    avail_analysis = st.session_state['oracle_loaded_snap']['avail']

# 1. SOB 데이터 처리
if sob_files:
    try:
        for f in sob_files:
            dfs = robust_read_all_sheets(f)
            for df_full in dfs:
                if df_full.empty: continue
                header_idx = -1
                for i in range(min(20, len(df_full))):
                    row_str = str(df_full.iloc[i].values).replace(' ', '')
                    if ('일자' in row_str or '날짜' in row_str) and ('매출' in row_str or '점유율' in row_str):
                        header_idx = i; break
                
                if header_idx != -1:
                    df_data = df_full.iloc[header_idx+1:].copy()
                    df_data.columns = deduplicate_columns(df_full.iloc[header_idx].values)
                    c_date = find_column(df_data, ['일자', '날짜', 'Date'])
                    c_occ = find_column(df_data, ['점유율', 'Occ'])
                    c_rev = find_column(df_data, ['매출', 'Revenue']) 
                    c_rn = find_column(df_data, ['객실수', 'RN']) 
                    c_adr = find_column(df_data, ['객단가', 'ADR']) 
                    
                    if c_date and c_rev:
                        f_m = None
                        for val in df_data[c_date].astype(str):
                            match = re.match(r'202\d-(\d{2})-\d{2}', val)
                            if match: f_m = int(match.group(1)); break
                        
                        if f_m:
                            df_clean = df_data.dropna(subset=[c_date], how='all')
                            sum_rows = df_clean[df_clean[c_date].astype(str).str.contains('합계|총계|Total', na=False)]
                            target_row = None
                            if not sum_rows.empty: target_row = sum_rows.iloc[-1]
                            else:
                                for idx in range(len(df_clean)-1, -1, -1):
                                    if clean_numeric(df_clean.iloc[idx][c_rev]) > 0:
                                        target_row = df_clean.iloc[idx]; break
                                        
                            if target_row is not None:
                                rev_val = clean_numeric(target_row[c_rev])
                                occ_val = clean_numeric(target_row[c_occ]) if c_occ else 0.0
                                rn_val = clean_numeric(target_row[c_rn]) if c_rn else 0.0
                                adr_val = clean_numeric(target_row[c_adr]) if c_adr else (rev_val/rn_val if rn_val>0 else 0)

                                if rev_val > yearly_data_store[f_m]['rev']:
                                    yearly_data_store[f_m] = {"rev": rev_val, "occ": occ_val, "rn": rn_val, "adr": adr_val}
        st.sidebar.success("✅ 최신 SOB 데이터로 업데이트 완료")
    except Exception as e: st.sidebar.error(f"SOB 처리 실패: {e}")

# 2. 객실 가용(Avail) 데이터 처리
if avail_files:
    try:
        avail_history = []
        for f in avail_files:
            dfs = robust_read_all_sheets(f)
            for df_a in dfs:
                if df_a.empty: continue
                up_date = extract_date_from_avail(df_a, f.name)
                type_idx = -1
                for i in range(min(15, len(df_a))):
                    if '객실타입' in str(df_a.iloc[i].values).replace(' ', ''):
                        type_idx = i; break
                
                if type_idx != -1:
                    d_headers = df_a.iloc[type_idx - 1].values[2:]
                    for i in range(type_idx + 1, len(df_a)):
                        r_type = str(df_a.iloc[i, 0]).strip()
                        if pd.isna(r_type) or r_type in ['nan', '합계', '예약객실', 'None', '']: continue
                        max_cap = clean_numeric(df_a.iloc[i, 1])
                        for j, d_str in enumerate(d_headers):
                            if pd.isna(d_str) or str(d_str).strip() in ['', 'nan']: continue
                            rem_val = clean_numeric(df_a.iloc[i, 2+j])
                            occ_val = ((max_cap - rem_val) / max_cap * 100) if max_cap > 0 else 0
                            clean_date = str(d_str).replace('.0', '').strip()
                            avail_history.append({"update_at": up_date, "date": f"2026-{clean_date}", "type": r_type, "occ": occ_val})
        
        df_h = pd.DataFrame(avail_history)
        if not df_h.empty:
            updates = sorted(df_h['update_at'].unique())
            if len(updates) >= 2:
                l_up, p_up = updates[-1], updates[-2]
                df_l, df_p = df_h[df_h['update_at'] == l_up], df_h[df_h['update_at'] == p_up]
                merged = pd.merge(df_l, df_p, on=['date', 'type'], suffixes=('_new', '_old'))
                merged['velocity'] = merged['occ_new'] - merged['occ_old']
                merged['suggested_tier'] = merged.apply(lambda row: get_dynamic_bar_tier(row['occ_new'], row['date']), axis=1)
                avail_analysis = merged[merged['velocity'] != 0].to_dict('records')
                st.sidebar.success("✅ 최신 재고 가속도 업데이트 완료")
    except Exception as e: st.sidebar.error(f"재고 분석 에러: {e}")

# ======================================================================
# 🚀 [아키텍트 엔진 v6.9] PMS 증분 병합 (채널/거래처 데이터 보존)
# ======================================================================
if pms_files:
    try:
        new_pms_list = []
        for f in pms_files:
            try:
                raw = pd.read_csv(f, encoding='cp949', header=None) if f.name.endswith('.csv') else pd.read_excel(f, header=None)
                h_idx = -1
                for i in range(min(25, len(raw))):
                    if '입실일자' in "".join([str(x) for x in raw.iloc[i].values]):
                        h_idx = i; break
                if h_idx != -1:
                    df_d = raw.iloc[h_idx+1:].copy()
                    df_d.columns = deduplicate_columns(raw.iloc[h_idx].values)
                    new_pms_list.append(df_d)
            except: pass
    
        if new_pms_list:
            new_v_df = pd.concat(new_pms_list, ignore_index=True)
            
            st_col = find_column(new_v_df, ['상태', 'Status'])
            if st_col:
                new_v_df = new_v_df[~new_v_df[st_col].astype(str).str.contains('RC|취소|Cancel|NoShow', case=False, na=False)]
            
            c_in = find_column(new_v_df, ['입실일자', '체크인'])
            c_rn = find_column(new_v_df, ['박수', '숙박일수', 'RN'])
            c_rooms = find_column(new_v_df, ['객실수', 'RoomCount'])
            c_room_rev = find_column(new_v_df, ['객실료', '객실매출'])
            c_tp = find_column(new_v_df, ['객실타입', 'RoomType'])
            c_id = find_column(new_v_df, ['예약번호', 'ConfNo', 'No', '예약번호 '])
            c_bk = find_column(new_v_df, ['예약일자', '예약일'])
            # 💡 [추가] Q열에 있는 '거래처' 컬럼을 찾아서 변수에 저장합니다!
            c_ch = find_column(new_v_df, ['거래처', '예약처', 'Channel', '예약경로'])

            new_v_df['Temp_In'] = pd.to_datetime(new_v_df[c_in], errors='coerce')
            new_v_df['Val_Nights'] = pd.to_numeric(new_v_df[c_rn].astype(str).str.extract(r'(\d+)')[0], errors='coerce').fillna(1).astype(int)
            new_v_df['Val_Rooms'] = pd.to_numeric(new_v_df[c_rooms].astype(str).str.extract(r'(\d+)')[0], errors='coerce').fillna(1).astype(int) if c_rooms else 1
            new_v_df['Rate_Per_Night'] = pd.to_numeric(new_v_df[c_room_rev].astype(str).str.replace(r'[^-0-9.]', '', regex=True), errors='coerce').fillna(0)
            
            if c_bk: new_v_df['Temp_Bk'] = pd.to_datetime(new_v_df[c_bk], errors='coerce')
            else: new_v_df['Temp_Bk'] = new_v_df['Temp_In'] - pd.Timedelta(days=1)
            new_v_df['Temp_Bk'] = new_v_df['Temp_Bk'].fillna(new_v_df['Temp_In'] - pd.Timedelta(days=1))

            new_v_df = new_v_df.dropna(subset=['Temp_In', c_tp])

            def expand_v69(row):
                unit_daily_rev = row['Rate_Per_Night'] / row['Val_Rooms'] if row['Val_Rooms'] > 0 else 0
                res_id = str(row[c_id]).strip() if c_id and pd.notna(row[c_id]) else f"{row[c_tp]}_{row['Rate_Per_Night']}"
                # 💡 [추가] 거래처 데이터가 있으면 빼내고, 없으면 '기타'로 둡니다.
                ch_name = str(row[c_ch]).strip() if c_ch and pd.notna(row[c_ch]) else "기타 (Unknown)"
                
                rows = []
                for n in range(row['Val_Nights']):
                    current_date = row['Temp_In'] + pd.Timedelta(days=n)
                    for r in range(row['Val_Rooms']):
                        rows.append({
                            'Stay_Date': current_date,
                            'Daily_Rev': unit_daily_rev,
                            'Daily_RN': 1.0,
                            '객실타입': row[c_tp],
                            'Temp_In': row['Temp_In'],
                            'Temp_Bk': row['Temp_Bk'],
                            'Channel': ch_name,  # 💡 [추가] 분해된 데이터 표에 거래처 이름도 같이 담아줍니다!
                            'Unique_Key': f"{res_id}_{current_date.strftime('%Y%m%d')}_R{r}"
                        })
                return rows
            
            new_expanded = pd.DataFrame([item for sublist in new_v_df.apply(expand_v69, axis=1) for item in sublist])
            
            if not df_full_pms.empty:
                if 'Unique_Key' not in df_full_pms.columns:
                    df_full_pms['Unique_Key'] = df_full_pms['객실타입'] + "_" + df_full_pms['Stay_Date'].astype(str)
                combined_pms = pd.concat([df_full_pms, new_expanded], ignore_index=True)
            else:
                combined_pms = new_expanded

            df_full_pms = combined_pms.drop_duplicates(subset=['Unique_Key'], keep='last').reset_index(drop=True)

            st.sidebar.success(f"✅ PMS 증분 업데이트 완료: 총 {len(df_full_pms):,} RN 원자적 분해 완료")

    except Exception as e:
        st.sidebar.error(f"PMS 분석 오류: {e}")
        
# ==========================================
# 공통 지표 연산
# ==========================================
if not df_full_pms.empty:
    for col in ['Stay_Date', 'Temp_Bk', 'Temp_In', 'Temp_Out']:
        if col in df_full_pms.columns:
            df_full_pms[col] = pd.to_datetime(df_full_pms[col], errors='coerce')

    try:
        c_tp = find_column(df_full_pms, ['객실타입', 'RoomType'])
        c_path = find_column(df_full_pms, ['예약경로', 'Source'])
        
        num_d = calendar.monthrange(2026, selected_month)[1]
        t_dates_m = pd.date_range(start=f"2026-{selected_month:02d}-01", end=f"2026-{selected_month:02d}-{num_d}")
        
        target_df = df_full_pms[df_full_pms['Stay_Date'].dt.month == selected_month].copy()
        
        if not target_df.empty:
            daily_r = target_df.groupby('Stay_Date')['Daily_Rev'].sum().reset_index()
            acc = 0.0; temp_p = []
            for d in t_dates_m:
                acc += daily_r[daily_r['Stay_Date'] == d]['Daily_Rev'].sum() / 100000000
                temp_p.append(acc)
            actual_pace = temp_p
            
            target_df['LeadTime'] = (target_df['Stay_Date'] - target_df['Temp_Bk']).dt.days
            actual_curve = [target_df[target_df['LeadTime'] >= -d]['Daily_Rev'].sum() / 100000000 for d in np.arange(-90, 1)]
            
            if c_tp:
                real_room_df = target_df.groupby(c_tp).agg({
                    'Daily_Rev': 'sum', 
                    'Daily_RN': 'sum'
                }).reset_index()
                
                real_room_df['평균 ADR'] = (real_room_df['Daily_Rev'] / real_room_df['Daily_RN']).fillna(0)
                
                real_room_df.rename(columns={
                    c_tp: '객실타입', 
                    'Daily_Rev': '총 객실매출', 
                    'Daily_RN': '판매 객실수(RN)'
                }, inplace=True)
            
            if c_path:
                real_channel_df = target_df.groupby(c_path)['Daily_Rev'].sum().reset_index()

    except Exception as e: 
        pass

# ==========================================
# 사이드바 (하단) - 🔥 Firebase 기반 클라우드 타임머신
# ==========================================
st.sidebar.markdown("---")
st.sidebar.subheader("🔥 Firebase 클라우드 백업")

snap_name = st.sidebar.text_input("💾 데이터 백업 이름", value=f"{datetime.now(timezone(timedelta(hours=9))).strftime('%m/%d %H:%M')} 마스터 백업")
if st.sidebar.button("📤 현재 전체 데이터를 Firebase에 백업", use_container_width=True):
    if not df_full_pms.empty or any(v['rev'] > 0 for v in yearly_data_store.values()):
        save_to_cloud(snap_name, df_full_pms, yearly_data_store, avail_analysis)
    else:
        st.sidebar.warning("저장할 데이터가 없습니다.")

with st.sidebar.expander("📥 과거 백업 관리 및 시스템 초기화", expanded=False):
    pick_date = st.date_input("조회/삭제할 백업 날짜 선택", value=datetime.now(timezone(timedelta(hours=9))))
    snaps_today = get_snapshots_by_date(pick_date)

    if snaps_today:
        snap_opts = {s['id']: s['name'] for s in snaps_today}
        sel_snap_id = st.selectbox("해당 날짜의 백업 시점 선택", options=list(snap_opts.keys()), format_func=lambda x: snap_opts[x])
        
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🔄 백업 불러오기", use_container_width=True):
                pms_c, sob_c, avail_c = load_snapshot_data(sel_snap_id)
                st.session_state['oracle_loaded_snap'] = {'pms': pms_c, 'sob': sob_c, 'avail': avail_c}
                st.rerun()
        with c2:
            if st.button("🗑️ 이 백업만 삭제", use_container_width=True):
                if delete_snapshot(sel_snap_id):
                    st.success("Firebase에서 해당 백업이 영구 삭제되었습니다.")
                    st.rerun()
    else:
        st.info(f"📅 {pick_date.strftime('%Y-%m-%d')}에는 저장된 백업이 없습니다.")

    st.markdown("---")
    if st.button("🧨 시스템 완전 초기화 (모든 데이터 리셋)", use_container_width=True):
        st.session_state['oracle_loaded_snap'] = None
        st.session_state['oracle_file_key'] += 1
        st.rerun()

# 🚨 긴급 DB 정리 기능
with st.sidebar.expander("🚨 긴급 Firebase 정리 (용량 확보)", expanded=False):
    st.caption("⚠️ 최근 N개만 남기고 나머지 전부 삭제합니다.")
    keep_n = st.number_input("최근 몇 개를 남길까요?", min_value=5, max_value=50, value=10)
    if st.button("🗑️ 오래된 백업 일괄 삭제", type="secondary"):
        with st.spinner("정리 중..."):
            deleted = emergency_cleanup_firebase(keep_recent=keep_n)
            if deleted > 0:
                st.success(f"✅ {deleted}개 삭제 완료!")
            else:
                st.info("삭제할 백업이 없습니다.")
            st.rerun()


with st.sidebar.expander("📊 2026년 마스터 타겟 보드 (항시 열람)", expanded=False):
    tgt_df = pd.DataFrame.from_dict(TARGET_DATA, orient='index')
    tgt_df.index.name = '월'
    tgt_df.rename(columns={'rn': '목표 RN', 'adr': '목표 ADR', 'occ': '목표 OCC(%)', 'rev': '목표 매출'}, inplace=True)
    tgt_df.loc['합계'] = [27117, 375346, 55.7, 10179268802]
    
    styled_tgt = tgt_df.style.format({
        '목표 RN': '{:,.0f}',
        '목표 ADR': '{:,.0f}',
        '목표 OCC(%)': '{:,.1f}',
        '목표 매출': '{:,.0f}'
    })
    st.dataframe(styled_tgt, use_container_width=True)

# 🚨 상단 지표 하드코딩 OTB 동기화
FACT_DB_GLOBAL = {
    4: {1: 666606568, 2: 680240552, 3: 683484877, 6: 706396340, 7: 713650569, 8: 725514271, 9: 732471320, 10: 729130460, 13: 752906651},
    5: {1: 580174512, 2: 584284522, 3: 589896496, 6: 604640008, 7: 617226508, 8: 630307581, 9: 638878045, 10: 646880667, 13: 677498662},
    6: {1: 317608189, 2: 323004791, 3: 325341332, 6: 329998237, 7: 336899555, 8: 354565622, 9: 355016508, 10: 357755106, 13: 360980571}
}
for m_fact in FACT_DB_GLOBAL:
    if m_fact in FACT_DB_GLOBAL and FACT_DB_GLOBAL[m_fact]:
        max_v = max(FACT_DB_GLOBAL[m_fact].values())
        if max_v > yearly_data_store[m_fact]['rev']:
            yearly_data_store[m_fact]['rev'] = max_v

# ==========================================
# 4. 메인 대시보드 화면 구성
# ==========================================
cur_data = yearly_data_store[selected_month]
current_rev_total = cur_data['rev']
current_occ_pct = cur_data['occ']
current_rn_total = cur_data['rn']
current_adr_actual = cur_data['adr']

if current_rev_total == 0 and not df_full_pms.empty:
    try:
        m_df = df_full_pms[df_full_pms['Stay_Date'].dt.month == selected_month]
        if not m_df.empty:
            current_rev_total = float(m_df['Daily_Rev'].sum())
            current_rn_total = float(m_df['Daily_RN'].sum())
            current_adr_actual = current_rev_total / current_rn_total if current_rn_total > 0 else 0
            
            num_days = calendar.monthrange(2026, selected_month)[1]
            t_cap = TOTAL_ROOM_CAPACITY * num_days
            current_occ_pct = (current_rn_total / t_cap * 100) if t_cap > 0 else 0.0
    except: pass
        
st.title("🏛️ AMBER ORACLE v5.5")
st.subheader("Revenue Architect Strategic War Room | Firebase Cloud Mode 🔥")
st.markdown("---")

y_cols = st.columns(6)
for i in range(12):
    m = i + 1; m_data = yearly_data_store[m]; bud = TARGET_DATA[m]['rev']
    with y_cols[i % 6]:
        p = (m_data['rev'] / bud * 100) if bud > 0 else 0
        st.metric(f"{m}월 Revenue", f"₩{m_data['rev']/1000000:.0f}M", f"{p:.1f}% 달성")
st.markdown("---")

tgt_m = TARGET_DATA[selected_month]
st.markdown(f"### 🎯 {selected_month}월 4D 목표 대비 실적 (Target vs Actual)")
k1, k2, k3, k4 = st.columns(4)

with k1: 
    prog_rev = (current_rev_total/tgt_m['rev']*100) if tgt_m['rev']>0 else 0
    st.metric("Total Revenue (OTB)", f"₩{current_rev_total:,.0f}", f"목표 ₩{tgt_m['rev']:,.0f} ({prog_rev:.1f}%)")
with k2: 
    prog_rn = (current_rn_total/tgt_m['rn']*100) if tgt_m['rn']>0 else 0
    st.metric("Room Nights (RN)", f"{current_rn_total:,.0f} RN", f"목표 {tgt_m['rn']:,} RN ({prog_rn:.1f}%)")
with k3: 
    adr_diff = current_adr_actual - tgt_m['adr']
    st.metric("Actual ADR", f"₩{current_adr_actual:,.0f}", f"목표대비 ₩{adr_diff:,.0f}", delta_color="normal")
with k4: 
    occ_diff = current_occ_pct - tgt_m['occ']
    st.metric("Occupancy (OCC)", f"{current_occ_pct:.1f}%", f"목표대비 {occ_diff:.1f}%p", delta_color="normal")

st.markdown("---")

tabs = st.tabs([
    "🚀 페이스", "🏢 객실 감사", "🔗 시장", "💸 채널", 
    "🔮 예보", "🌟 리뷰", "🛰️ 감시", "⚔️ 대조(결론)", "🔮 AI 제안", "🎯 단가 조정"
])

with tabs[0]:
    st.subheader(f"📊 {selected_month}월 예약 가속도 모니터링 (Fact-Check Dashboard)")
    st.info("💡 **[아키텍트 팩트 동기화]** 파일 내부에서 '영업월'을 자동 판독하고, 파일명의 다운로드 날짜를 추적하여 OTB를 연장합니다.")
    
    num_d = calendar.monthrange(2026, selected_month)[1]
    t_dt = pd.date_range(start=f"2026-{selected_month:02d}-01", end=f"2026-{selected_month:02d}-{num_d}")
    start_trace = t_dt[0] - pd.DateOffset(months=3)
    trace_dt = pd.date_range(start=start_trace, end=t_dt[-1])
    
    kst_now = datetime.now(timezone(timedelta(hours=9)))
    today_date = kst_now.replace(tzinfo=None)
    curr_d = today_date.day if today_date.month == selected_month else (num_d if today_date.month > selected_month else 1)
    cur_idx = int(curr_d - 1) if curr_d <= num_d else -1
    
    FACT_DB = {
        4: {1: 666606568, 2: 680240552, 3: 683484877, 6: 706396340, 7: 713650569, 8: 725514271, 9: 732471320, 10: 729130460, 13: 752906651},
        5: {1: 580174512, 2: 584284522, 3: 589896496, 6: 604640008, 7: 617226508, 8: 630307581, 9: 638878045, 10: 646880667, 13: 677498662},
        6: {1: 317608189, 2: 323004791, 3: 325341332, 6: 329998237, 7: 336899555, 8: 354565622, 9: 355016508, 10: 357755106, 13: 360980571}
    }
    
    daily_otb_dict = {}
    if selected_month in FACT_DB:
        for day_k, val in FACT_DB[selected_month].items():
            daily_otb_dict[day_k] = val / 100000000

    if sob_files:
        for f in sob_files:
            try:
                f.seek(0)
                raw_sob = pd.read_excel(f, header=None) if f.name.endswith('.xlsx') else pd.read_csv(f, encoding='cp949', header=None)
                
                content_month = None
                for i in range(min(15, len(raw_sob))):
                    row_str = "".join([str(x) for x in raw_sob.iloc[i].values]).replace(' ', '')
                    m_match = re.search(r'202\d-(\d{2})', row_str)
                    if m_match:
                        content_month = int(m_match.group(1))
                        break
                
                if content_month == selected_month:
                    date_match = re.search(r'\d{8}', f.name)
                    update_day = int(date_match.group()[-2:]) if date_match else curr_d
                    
                    max_val = 0
                    for r_idx in range(len(raw_sob)):
                        nums = [clean_numeric(x) for x in raw_sob.iloc[r_idx].values]
                        if max(nums) > max_val: max_val = max(nums)
                    
                    if max_val > 100000000:
                        daily_otb_dict[update_day] = max_val / 100000000
            except: pass

    booking_pace_m = []
    velocity = 0
    if daily_otb_dict:
        actual_last_day = max(daily_otb_dict.keys())
        last_val = 0
        for d in range(1, actual_last_day + 1):
            if d in daily_otb_dict:
                current_val = daily_otb_dict[d]
                if last_val > 0 and current_val < last_val * 0.8:
                    pass
                else:
                    last_val = current_val
            booking_pace_m.append(last_val)
        
        cur_rev_sob = booking_pace_m[-1] * 100000000 if booking_pace_m else 0
        if len(booking_pace_m) >= 8:
            velocity = ((booking_pace_m[-1] - booking_pace_m[-8]) / 7) * 100000000
    else:
        cur_rev_sob = 0

    tgt_m = TARGET_DATA.get(selected_month, {"rev": 0, "rn": 0, "adr": 0, "occ": 0})
    tgt_rev_100m = tgt_m['rev'] / 100000000
    base_otb_ratio = 0.50
    days_arr = np.arange(1, num_d + 1)
    pacing_curve_ratio = base_otb_ratio + (1 - base_otb_ratio) * ((days_arr / num_d) ** 0.6)
    o_p = tgt_rev_100m * pacing_curve_ratio
    u_b, l_b = o_p * 1.08, o_p * 0.92

    booking_evolution = []
    actual_curve = []
    if not df_full_pms.empty:
        t_df = df_full_pms[df_full_pms['Stay_Date'].dt.month == selected_month].copy()
        
        if 'Temp_Bk' in t_df.columns and not t_df.empty:
            for d in trace_dt:
                if d > today_date: break
                check_ts = d.replace(hour=23, minute=59, second=59)
                evol_sum = t_df[t_df['Temp_Bk'] <= check_ts]['Daily_Rev'].sum()
                booking_evolution.append(evol_sum / 100000000)
            
            t_df['LeadTime'] = (t_df['Stay_Date'] - t_df['Temp_Bk']).dt.days
            for d in np.arange(-90, 1):
                curve_sum = t_df[t_df['LeadTime'] >= -d]['Daily_Rev'].sum()
                actual_curve.append(curve_sum / 100000000)

    cur_rev = current_rev_total if current_rev_total > 0 else cur_rev_sob
    expected_pct = pacing_curve_ratio[cur_idx] if cur_idx != -1 else 1.0

    st.markdown(f"### 🧭 OTB 궤도 검증 (데이터 업데이트: {max(daily_otb_dict.keys()) if daily_otb_dict else '팩트'}일 기준)")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("순수 객실 매출 (OTB Fact)", f"{int(cur_rev):,} 원")
    m2.metric("세이프존 기준점", f"{int(o_p[cur_idx]*100000000):,} 원" if cur_idx != -1 else "-")
    m3.metric("최근 7일 일평균 픽업", f"{int(velocity):,} 원/일")
    m4.metric("월말 예상 마감", f"{int(cur_rev / expected_pct):,} 원")
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 1️⃣ 실투숙 누적 궤도 (Stay Pace)")
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=t_dt, y=[tgt_rev_100m*(i/num_d) for i in range(1, num_d+1)], name="Target", line=dict(color="gray", dash='dot')))
        if len(actual_pace) > 0:
            fig1.add_trace(go.Scatter(x=t_dt[:curr_d], y=actual_pace[:curr_d], name="Actual (PMS Net)", line=dict(color="#00D1FF", width=4)))
        st.plotly_chart(fig1.update_layout(template="plotly_dark", height=300, margin=dict(l=10, r=10, t=30, b=10)), use_container_width=True)
        
    with c2:
        st.markdown("#### 2️⃣ 당월 확보 매출 궤도 (Booking Pace)")
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=t_dt, y=l_b, mode='lines', line_width=0, fill='tonexty', fillcolor='rgba(0,209,255,0.1)', name="Safe Zone"))
        fig2.add_trace(go.Scatter(x=t_dt, y=o_p, name="Oracle S-Curve", line=dict(color="#00D1FF", width=2)))
        if booking_pace_m: 
            plot_x = t_dt[:len(booking_pace_m)]
            fig2.add_trace(go.Scatter(x=plot_x, y=booking_pace_m, name="Actual (SOB)", line=dict(color="#FF4B4B", width=4)))
        st.plotly_chart(fig2.update_layout(template="plotly_dark", height=300, margin=dict(l=10, r=10, t=30, b=10)), use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("#### 3️⃣ 3개월 전부터의 매출 진화 (Evolution)")
        fig3 = go.Figure()
        if booking_evolution:
            fig3.add_trace(go.Scatter(x=trace_dt[:len(booking_evolution)], y=booking_evolution, name="Build-up (PMS)", line=dict(color="#FFD700", width=3)))
        st.plotly_chart(fig3.update_layout(template="plotly_dark", height=300, margin=dict(l=10, r=10, t=30, b=10)), use_container_width=True)

    with c4:
        st.markdown("#### ⏳ 4️⃣ 리드타임별 예약 곡선 (D-90)")
        fig4 = go.Figure()
        _, t_c = get_booking_curve(tgt_rev_100m, 90, 1.0)
        fig4.add_trace(go.Scatter(x=np.arange(-90, 1), y=t_c, name="Standard", line=dict(color="gray", dash='dash')))
        if actual_curve:
            fig4.add_trace(go.Scatter(x=np.arange(-90, 1), y=actual_curve, name="Actual (PMS)", line=dict(color='#FF4B4B', width=4)))
        st.plotly_chart(fig4.update_layout(template="plotly_dark", height=300, margin=dict(l=10, r=10, t=30, b=10)), use_container_width=True)
        
with tabs[1]:
    st.subheader("🏢 타입별 객실 매출 및 유료 ADR 정밀 감사")
    st.info("💡 **[Architect Logic]** 매출 0원(무료/컴프) 객실은 ADR 계산에서 제외하며, '무료 RN' 항목으로 별도 집계합니다.")
    
    if df_full_pms is not None and not df_full_pms.empty:
        audit_df = df_full_pms[df_full_pms['Stay_Date'].dt.month == selected_month].copy()
        
        if not audit_df.empty:
            room_audit = audit_df.groupby('객실타입').apply(lambda x: pd.Series({
                '총 객실매출': x['Daily_Rev'].sum(),
                '유료 RN': (x['Daily_Rev'] > 0).sum(),
                '무료 RN': (x['Daily_Rev'] == 0).sum(),
                '전체 RN': len(x)
            })).reset_index()
            
            room_audit['유료 ADR'] = (room_audit['총 객실매출'] / room_audit['유료 RN']).replace([np.inf, -np.inf], 0).fillna(0)
            
            room_audit = room_audit.sort_values(by='총 객실매출', ascending=False)
            
            display_cols = ['객실타입', '총 객실매출', '유료 RN', '무료 RN', '전체 RN', '유료 ADR']
            st.dataframe(room_audit[display_cols].style.format({
                '총 객실매출': '₩{:,.0f}', 
                '유료 RN': '{:,.0f}', 
                '무료 RN': '{:,.0f}', 
                '전체 RN': '{:,.0f}', 
                '유료 ADR': '₩{:,.0f}'
            }), use_container_width=True, height=400)
            
            with st.expander("🔍 타입별 유료/무료 투숙 리스트 상세 대조"):
                target_type = st.selectbox("검증할 타입", room_audit['객실타입'].unique())
                detail_view = audit_df[audit_df['객실타입'] == target_type].copy()
                detail_view['매출구분'] = detail_view['Daily_Rev'].apply(lambda x: '유료' if x > 0 else '무료(0원)')
                
                st.write(f"📊 **{target_type}** 상세 내역 (총 {len(detail_view)}박)")
                st.dataframe(detail_view[['Stay_Date', 'Daily_Rev', '매출구분']].sort_values('Stay_Date'), use_container_width=True)
                
        else:
            st.warning(f"{selected_month}월 투숙 데이터가 없습니다.")
    else:
        st.info("PMS 데이터를 업로드해 주세요.")
        
    with tabs[2]:
        st.subheader(f"⚖️ {selected_month}월 수요(RN) vs 단가(ADR) 전략 매트릭스")
        st.info("💡 **[Price vs Demand]** 객실 판매량(RN)을 채우기 위해 단가(ADR)를 얼마나 훼손하고 있는지 확인하는 실시간 이중축 차트입니다.")
        
        if not df_full_pms.empty:
            t2_df = df_full_pms[df_full_pms['Stay_Date'].dt.month == selected_month].copy()
            if not t2_df.empty:
                daily_perf = t2_df.groupby(t2_df['Stay_Date'].dt.date).agg(RN=('Daily_RN', 'sum'), Rev=('Daily_Rev', 'sum')).reset_index()
                daily_perf['ADR'] = daily_perf['Rev'] / daily_perf['RN'].replace(0, 1)
                
                from plotly.subplots import make_subplots
                fig3 = make_subplots(specs=[[{"secondary_y": True}]])
                fig3.add_trace(go.Bar(x=daily_perf['Stay_Date'], y=daily_perf['RN'], name="판매 객실(RN)", marker_color='rgba(0, 209, 255, 0.4)'), secondary_y=False)
                fig3.add_trace(go.Scatter(x=daily_perf['Stay_Date'], y=daily_perf['ADR'], name="평균 단가(ADR)", mode='lines+markers', line=dict(color='#FFD700', width=3)), secondary_y=True)
                
                fig3.update_layout(template="plotly_dark", height=400, hovermode="x unified")
                fig3.update_yaxes(title_text="객실 수 (RN)", secondary_y=False, showgrid=False)
                fig3.update_yaxes(title_text="단가 (원)", secondary_y=True, showgrid=True)
                st.plotly_chart(fig3, use_container_width=True)
            else:
                st.warning("해당 월의 예약 데이터가 없습니다.")
        else:
            st.warning("PMS 데이터가 업로드되지 않았습니다.")

    with tabs[3]:
        st.subheader("🍕 채널별 수익 기여도 (Channel Mix & Profitability)")
        st.info("💡 **[Channel Mix]** 어떤 거래처가 가장 돈이 되는지(매출 점유율)를 직관적으로 분석합니다.")
        
        # 💡 국내외 PMS에서 자주 쓰는 채널 컬럼명 후보군을 모두 탐색합니다.
        possible_cols = ['거래처', '예약처', '거래처명', '채널', '예약경로', 'Channel', 'Agency', 'Agent', 'Source', '대행사']
        ch_col = None
        
        if not df_full_pms.empty:
            for col in possible_cols:
                if col in df_full_pms.columns:
                    ch_col = col
                    break
        
        if ch_col and not df_full_pms.empty:
            ch_df = df_full_pms[df_full_pms['Stay_Date'].dt.month == selected_month].groupby(ch_col)['Daily_Rev'].sum().reset_index()
            ch_df = ch_df[ch_df['Daily_Rev'] > 0]
            
            if not ch_df.empty:
                # 거래처가 너무 많을 경우를 대비해 매출액 기준 상위 15개만 묶어서 보여줍니다.
                top_ch = ch_df.nlargest(15, 'Daily_Rev')
                if len(ch_df) > 15:
                    other_rev = ch_df[~ch_df[ch_col].isin(top_ch[ch_col])]['Daily_Rev'].sum()
                    other_df = pd.DataFrame({ch_col: ['기타 (Others)'], 'Daily_Rev': [other_rev]})
                    final_ch_df = pd.concat([top_ch, other_df])
                else:
                    final_ch_df = top_ch
                
                fig4 = px.treemap(final_ch_df, path=[ch_col], values='Daily_Rev', color='Daily_Rev', 
                                  color_continuous_scale='Viridis', title=f"[{ch_col}] 기준 매출 점유율 (Treemap)")
                fig4.update_layout(template="plotly_dark", height=450, margin=dict(t=50, l=10, r=10, b=10))
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.warning("분석할 채널 실적이 아직 없습니다.")
        else:
            st.warning("⚠️ 업로드하신 PMS 파일에서 '채널/거래처'를 나타내는 컬럼명을 자동으로 찾지 못했습니다.")
            with st.expander("🔍 내 엑셀 파일의 실제 컬럼명 확인하기 (클릭)"):
                if not df_full_pms.empty:
                    st.write("아래 목록 중 채널(예: 아고다, 네이버 등)이 들어있는 컬럼이름을 확인해주세요.")
                    st.info(list(df_full_pms.columns))
                    st.caption("확인하신 후 코드의 `possible_cols` 목록에 그 이름을 추가하시면 차트가 정상적으로 뜹니다!")

    with tabs[4]:
        st.subheader(f"🔮 {selected_month}월 매출 마감 예보 시뮬레이션")
        st.info("💡 **[Revenue Architect Forecast]** 단순 산술 평균이 아닌, 오라클 S-커브를 역산하여 남은 기간의 픽업 예상치를 더한 '정밀 마감 예보'입니다.")
        
        try:
            num_days = calendar.monthrange(2026, selected_month)[1]
            dates = pd.date_range(start=f"2026-{selected_month:02d}-01", periods=num_days)
            
            # [안전장치] 탭 0에서 계산된 변수들을 직접 안전하게 가져옵니다. (에러 원천 차단)
            safe_cur_rev = cur_rev if 'cur_rev' in dir() else 0
            safe_tgt_m = tgt_m if 'tgt_m' in dir() else {"rev": 100000000}
            safe_expected_pct = expected_pct if 'expected_pct' in dir() and expected_pct > 0 else 1.0
            safe_pacing_curve = pacing_curve_ratio if 'pacing_curve_ratio' in dir() else np.linspace(0.5, 1.0, num_days)
            safe_op = o_p if 'o_p' in dir() else np.linspace(0, safe_tgt_m['rev']/100000000, num_days)
            safe_booking_pace = booking_pace_m if 'booking_pace_m' in dir() else []

            cur_rev_unit = safe_cur_rev / 100000000 
            target_goal_unit = safe_tgt_m['rev'] / 100000000
            
            # 궤도 이탈률을 역산한 최종 예상 마감액
            forecast_final_unit = cur_rev_unit / safe_expected_pct if safe_expected_pct > 0 else cur_rev_unit

            forecast_line = [None] * num_days
            for i in range(num_days):
                forecast_line[i] = forecast_final_unit * safe_pacing_curve[i]

            fig_fcst = go.Figure()
            fig_fcst.add_trace(go.Scatter(x=dates, y=safe_op, name="Target (목표 곡선)", line=dict(color="rgba(0,209,255,0.3)", dash="dash")))
            
            if safe_booking_pace:
                fig_fcst.add_trace(go.Scatter(x=dates[:len(safe_booking_pace)], y=safe_booking_pace, name="Actual (현재 OTB)", line=dict(color="#00D1FF", width=4)))
            
            fig_fcst.add_trace(go.Scatter(x=dates, y=forecast_line, name="Oracle Forecast (마감 예측)", line=dict(color="#FFD700", width=2, dash="dot")))
            
            fig_fcst.update_layout(template="plotly_dark", height=450, yaxis_title="누적 매출 (억)")
            st.plotly_chart(fig_fcst, use_container_width=True)
            
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("현재 누적 매출 (OTB)", f"{cur_rev_unit:.2f} 억")
            with c2:
                target_diff = forecast_final_unit - target_goal_unit
                st.metric("월말 예상 매출 (Forecast)", f"{forecast_final_unit:.2f} 억", f"{target_diff:+.2f} 억")
            with c3:
                achievement_rate = (forecast_final_unit / target_goal_unit * 100) if target_goal_unit > 0 else 0
                st.metric("예상 달성률", f"{achievement_rate:.1f}%", delta=f"{achievement_rate-100:.1f}%p")

            st.success(f"📢 **오라클 브리핑:** 현재 기준, 목표 궤도상 전체 매출의 **{safe_expected_pct*100:.1f}%**가 확보되어야 하는 페이스입니다. 현재 OTB를 이 페이스로 역산하면 최종 마감은 **{forecast_final_unit:.2f}억**으로 예측되며, 이는 당초 목표 대비 **{achievement_rate:.1f}%** 수준입니다.")
            
        except Exception as e:
            st.error(f"예측 데이터를 구성하는 중 오류가 발생했습니다. (Tab 0의 데이터를 확인해주세요)")

with tabs[5]: st.subheader("🌟 리뷰 분석"); st.info("연동 대기 중")

with tabs[6]:
        st.subheader("🛰️ 외부 시장 지표 감시 및 매출 상관관계 (Market Correlation)")
        st.info("💡 Firebase에서 수집된 실제 크롤링 데이터(항공, 렌터카, 개별 경쟁사)를 가져와 투숙일(Stay Date) 기준으로 분석합니다.")
        
        if not df_full_pms.empty and firebase_admin._apps:
            target_df_corr = df_full_pms[df_full_pms['Stay_Date'].dt.month == selected_month].copy()
            if not target_df_corr.empty:
                daily_pms = target_df_corr.groupby(target_df_corr['Stay_Date'].dt.date).agg(
                    rev=('Daily_Rev', 'sum'),
                    rn=('Daily_RN', 'sum')
                ).reset_index()
                daily_pms.rename(columns={'Stay_Date': 'date'}, inplace=True)
                daily_pms['date'] = pd.to_datetime(daily_pms['date'])
                daily_pms['adr'] = daily_pms['rev'] / daily_pms['rn']
                daily_pms['adr'] = daily_pms['adr'].fillna(0)
                
                # 🔥 크롤링 데이터는 통합된 db_flight (firebase_flight) 앱에서 가져옵니다.
                flight_data, rental_data, comp_data = [], [], []
                month_prefix = f"2026-{selected_month:02d}"
                
                if db_flight:
                    try:
                        flights_ref = db_flight.collection('flight_prices').stream()
                        for doc in flights_ref:
                            d = doc.to_dict()
                            if d.get('date', '').startswith(month_prefix):
                                flight_data.append({'date': d.get('date'), 'flight_price': d.get('min_price', 0)})
                                
                        rentals_ref = db_flight.collection('rental_prices').stream()
                        for doc in rentals_ref:
                            d = doc.to_dict()
                            if d.get('date', '').startswith(month_prefix):
                                rental_data.append({'date': d.get('date'), 'rental_price': d.get('Ray_Price', 0)})
                                
                        comps_ref = db_flight.collection('hotel_comp_prices').stream()
                        for doc in comps_ref:
                            d = doc.to_dict()
                            if d.get('date', '').startswith(month_prefix):
                                comp_data.append({
                                    'date': d.get('date'), 
                                    'hotel_name': d.get('hotel_name', 'Unknown'), 
                                    'price': d.get('price', 0)
                                })
                    except Exception:
                        pass

                df_flight = pd.DataFrame(flight_data)
                if not df_flight.empty: df_flight['date'] = pd.to_datetime(df_flight['date'])
                
                df_rental = pd.DataFrame(rental_data)
                if not df_rental.empty: df_rental['date'] = pd.to_datetime(df_rental['date'])
                
                df_comp = pd.DataFrame(comp_data)
                if not df_comp.empty: 
                    df_comp['date'] = pd.to_datetime(df_comp['date'])
                    df_comp_pivot = df_comp.pivot_table(index='date', columns='hotel_name', values='price', aggfunc='mean').reset_index()
                else:
                    df_comp_pivot = pd.DataFrame()

                if not df_flight.empty: daily_pms = pd.merge(daily_pms, df_flight.groupby('date')['flight_price'].mean().reset_index(), on='date', how='left')
                else: daily_pms['flight_price'] = 0
                
                if not df_rental.empty: daily_pms = pd.merge(daily_pms, df_rental.groupby('date')['rental_price'].mean().reset_index(), on='date', how='left')
                else: daily_pms['rental_price'] = 0
                
                if not df_comp_pivot.empty: 
                    daily_pms = pd.merge(daily_pms, df_comp_pivot, on='date', how='left')
                
                for h in ['Parnas_Jeju', 'Grand_Josun', 'Amber_Pure_Hill']:
                    if h not in daily_pms.columns:
                        daily_pms[h] = 0

                daily_pms.ffill(inplace=True)
                daily_pms.fillna(0, inplace=True)

                st.markdown("#### 📈 실제 시장 요금 vs 엠버퓨어힐 매출 트렌드")
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Bar(x=daily_pms['date'], y=daily_pms['rev'], name="우리 매출(Gross)", opacity=0.4, yaxis='y1', marker_color='#00D1FF'))
                fig_trend.add_trace(go.Scatter(x=daily_pms['date'], y=daily_pms['flight_price'], name="평균 항공권", mode='lines+markers', yaxis='y2', line=dict(color='#4CAF50')))
                fig_trend.add_trace(go.Scatter(x=daily_pms['date'], y=daily_pms['rental_price'], name="평균 렌터카", mode='lines+markers', yaxis='y2', line=dict(color='#FFD700')))
                
                hotel_colors = {'Parnas_Jeju': '#FF4B4B', 'Grand_Josun': '#9370DB', 'Amber_Pure_Hill': '#FFFFFF'}
                hotel_labels = {'Parnas_Jeju': '파르나스', 'Grand_Josun': '그랜드조선', 'Amber_Pure_Hill': '엠버퓨어힐(크롤링)'}
                
                for h in ['Parnas_Jeju', 'Grand_Josun', 'Amber_Pure_Hill']:
                    if h in daily_pms.columns and not daily_pms[h].eq(0).all():
                        fig_trend.add_trace(go.Scatter(x=daily_pms['date'], y=daily_pms[h], name=hotel_labels.get(h, h), mode='lines+markers', yaxis='y2', line=dict(color=hotel_colors.get(h, '#9e2a2b'))))
                
                fig_trend.update_layout(
                    template="plotly_dark", height=450,
                    yaxis=dict(title="우측: 매출 (원)", side='right', showgrid=False),
                    yaxis2=dict(title="좌측: 시장 단가 (원)", overlaying='y', side='left', showgrid=True),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig_trend, use_container_width=True)
                
                st.markdown("#### 🔄 핵심 지표 상관계수 (Correlation Coefficient)")
                try:
                    corr_cols = ['rev', 'rn', 'adr', 'flight_price', 'rental_price'] + [h for h in ['Parnas_Jeju', 'Grand_Josun', 'Amber_Pure_Hill'] if h in daily_pms.columns]
                    corr_df = daily_pms[corr_cols].corr()
                    
                    c1, c2, c3 = st.columns(3)
                    corr_flight = corr_df.loc['rn', 'flight_price'] if 'flight_price' in corr_df else 0
                    corr_rent = corr_df.loc['rn', 'rental_price'] if 'rental_price' in corr_df else 0
                    
                    target_comp = 'Parnas_Jeju' if 'Parnas_Jeju' in corr_df else 'Grand_Josun'
                    corr_comp = corr_df.loc['adr', target_comp] if target_comp in corr_df else 0
                    comp_label = "파르나스" if target_comp == 'Parnas_Jeju' else "그랜드조선"
                    
                    def get_corr_text(val):
                        if pd.isna(val): return "데이터 부족"
                        if val > 0.7: return "매우 강한 양의 상관관계"
                        elif val > 0.3: return "양의 상관관계"
                        elif val > -0.3: return "상관관계 미미"
                        elif val > -0.7: return "음의 상관관계"
                        else: return "매우 강한 음의 상관관계"
                        
                    with c1: st.metric("✈️ 항공권 요금 vs 우리 호텔 판매량(RN)", f"{corr_flight:.2f}", get_corr_text(corr_flight), delta_color="off")
                    with c2: st.metric("🚗 렌터카 요금 vs 우리 호텔 판매량(RN)", f"{corr_rent:.2f}", get_corr_text(corr_rent), delta_color="off")
                    with c3: st.metric(f"🏨 {comp_label} 요금 vs 우리 호텔 ADR", f"{corr_comp:.2f}", get_corr_text(corr_comp), delta_color="off")
                        
                    st.markdown("---")
                    st.markdown("#### 🔬 상세 산점도 분석 (Scatter Plot)")
                    
                    x_options = ['flight_price', 'rental_price'] + [h for h in ['Parnas_Jeju', 'Grand_Josun', 'Amber_Pure_Hill'] if h in daily_pms.columns]
                    x_format = {'flight_price':'평균 항공권 요금', 'rental_price':'평균 렌터카 요금', 'Parnas_Jeju':'파르나스 요금', 'Grand_Josun':'그랜드조선 요금', 'Amber_Pure_Hill':'엠버퓨어힐(크롤링) 요금'}
                    
                    x_axis = st.selectbox("X축(원인) 지표 선택", x_options, format_func=lambda x: x_format.get(x, x))
                    y_axis = st.selectbox("Y축(결과) 지표 선택", ['rn', 'rev', 'adr'], format_func=lambda x: {'rn':'판매 객실수(RN)', 'rev':'총매출(Gross)', 'adr':'엠버퓨어힐 평균 ADR'}[x])
                    
                    if not daily_pms[x_axis].eq(0).all():
                        fig_scatter = px.scatter(daily_pms, x=x_axis, y=y_axis, template="plotly_dark", 
                                                 title=f"시장 지표에 따른 우리 호텔 실적 변화", opacity=0.7)
                        fig_scatter.update_traces(marker=dict(size=12, color='#00D1FF'))
                        st.plotly_chart(fig_scatter, use_container_width=True)
                    else:
                        st.warning("해당 지표의 시장 데이터가 아직 수집되지 않았습니다.")
                except Exception:
                    pass
            else:
                st.info("해당 월의 PMS 데이터가 부족하여 상관관계를 분석할 수 없습니다.")
        else:
            st.info("상관관계 분석을 위해 PMS 데이터를 먼저 업로드(또는 로드)해 주세요.")
            
with tabs[7]:
    st.markdown("---")
    st.subheader("📊 전략 보고서 정식 출력")
    if st.button("📄 회장님 보고용 종합 리포트 생성 (PDF)"):
        safe_adj_adr = int(sim_adr - current_adr_actual) if 'sim_adr' in locals() else 0
        safe_gain = int(ar_net - base_net) if ('ar_net' in locals() and 'base_net' in locals()) else 0
        
        report_payload = {
            'date': kst_now.strftime('%Y-%m-%d'),
            'month': selected_month,
            'act_rev': current_rev_total,
            'tgt_rev': tgt_m['rev'],
            'rev_pct': (current_rev_total / tgt_m['rev'] * 100) if tgt_m['rev'] > 0 else 0,
            'act_rn': current_rn_total,
            'tgt_rn': tgt_m['rn'],
            'rn_pct': (current_rn_total / tgt_m['rn'] * 100) if tgt_m['rn'] > 0 else 0,
            'act_adr': current_adr_actual,
            'tgt_adr': tgt_m['adr'],
            'adr_diff': int(current_adr_actual - tgt_m['adr']),
            'adj_adr': safe_adj_adr,
            'gain': safe_gain
        }
        
        try:
            pdf_data = export_comprehensive_report(report_payload)
            st.download_button(
                label="📥 PDF 리포트 다운로드",
                data=pdf_data,
                file_name=f"Amber_Strategy_Report_{selected_month}월.pdf",
                mime="application/pdf"
            )
            st.success("✅ 보고서가 성공적으로 생성되었습니다!")
        except Exception as e:
            st.error(f"❌ PDF 생성 실패: {e}")

    st.markdown("---")
    st.header(f"🏟️ {selected_month}월 수익 최적화 검증 (Architecture vs GM Policy)")
    
    st.subheader("1️⃣ 과거 가격 탄력성 검증 (Price Elasticity)")
    if not df_full_pms.empty:
        try:
            target_df = df_full_pms[df_full_pms['Stay_Date'].dt.month == selected_month].copy()
            elasticity_df = target_df.groupby('Stay_Date').agg({'Daily_Rev':'sum', 'Daily_RN':'sum'}).reset_index()
            elasticity_df['adr'] = elasticity_df['Daily_Rev'] / elasticity_df['Daily_RN']
            
            corr_val = elasticity_df['adr'].corr(elasticity_df['Daily_RN'])
            
            c_e1, c_e2 = st.columns([2, 1])
            with c_e1:
                fig_e = px.scatter(elasticity_df, x='adr', y='Daily_RN', trendline="ols", 
                                   title="우리 호텔 ADR 상승 시 물량 하락 변동성", template="plotly_dark")
                st.plotly_chart(fig_e, use_container_width=True)
            with c_e2:
                st.metric("가격 탄력성 지수", f"{corr_val:.2f}", "0에 가까울수록 가격저항 낮음" if corr_val > -0.3 else "가격저항 높음")
                st.write("💡 **분석 결과:**")
                if corr_val > -0.3: st.success("현재 단가를 더 올려도 물량 이탈이 적습니다. GM의 '박리다매'는 명백한 수익 손실입니다.")
                else: st.warning("가격 저항이 존재합니다. 단가 인상 시 정밀한 타겟 마케팅이 병행되어야 합니다.")
        except: st.info("탄력성 분석을 위한 데이터가 충분하지 않습니다.")

    st.subheader("2️⃣ 경쟁사 가격 격차 분석 (Price Gap Boundary)")
    try:
        comp_cols = [h for h in ['Parnas_Jeju', 'Grand_Josun'] if h in daily_pms.columns]
        if comp_cols:
            avg_comp_price = daily_pms[comp_cols].mean(axis=1).mean()
            price_gap = current_adr_actual - avg_comp_price
            
            col_g1, col_g2, col_g3 = st.columns(3)
            col_g1.metric("경쟁사 평균 요금", f"₩{int(avg_comp_price):,}")
            col_g2.metric("엠버 현재 ADR", f"₩{int(current_adr_actual):,}")
            col_g3.metric("가격 격차 (Gap)", f"₩{int(price_gap):,}", delta="시장 우위" if price_gap < 0 else "프리미엄 포지셔닝")
            
            if price_gap > 50000: st.error("🚨 경고: 경쟁사 대비 가격이 너무 높습니다. 예약 속도가 둔화될 임계점에 도달했습니다.")
            elif price_gap < -30000: st.success("📢 기회: 경쟁사 대비 저렴합니다. 즉시 단가를 상향하여 수익을 보전해야 합니다.")
    except: st.info("경쟁사 가격 격차를 분석할 크롤링 데이터가 없습니다.")

    st.subheader("3️⃣ 조기 완판 기회비용 (Opportunity Cost of Early Sellout)")
    V_C = 50000 
    
    if not df_full_pms.empty:
        c_tp = find_column(df_full_pms, ['객실타입', '룸타입', 'RoomType'])
        if c_tp:
            target_df['LeadTime'] = (target_df['Stay_Date'] - target_df['Temp_Bk']).dt.days
            target_df['Booking_ADR'] = target_df['Daily_Rev'] 
            
            def calculate_lost_revenue(row):
                if row['LeadTime'] <= 14: return 0.0
                if row['Booking_ADR'] < 150000: return 0.0
                
                r_type = str(row[c_tp]).strip()
                d_in = row['Stay_Date']
                
                try: type_code, season, is_weekend = get_season_details(d_in)
                except: return 0.0 
                
                base_price = 0
                if r_type in DYNAMIC_ROOMS:
                    base_price = PRICE_TABLE.get(r_type, {}).get("BAR8", 300000)
                elif r_type in FIXED_ROOMS:
                    base_price = FIXED_PRICE_TABLE.get(r_type, {}).get(type_code, 250000)
                else: 
                    base_price = 300000
                    
                floor_limit = base_price * 0.85 
                
                if row['Booking_ADR'] < floor_limit:
                    return (floor_limit - row['Booking_ADR']) * row['Daily_RN']
                return 0.0

            target_df['Lost_Revenue'] = target_df.apply(calculate_lost_revenue, axis=1)
            cheap_early_birds = target_df[target_df['Lost_Revenue'] > 0]
            
            early_rn = cheap_early_birds['Daily_RN'].sum()
            early_adr = cheap_early_birds['Booking_ADR'].mean() if early_rn > 0 else 0
            total_lost_revenue = cheap_early_birds['Lost_Revenue'].sum()

            c_l1, c_l2 = st.columns(2)
            with c_l1:
                st.metric("마지노선 이탈 덤핑 객실", f"{int(early_rn):,} RN", "적정 하한가 미달 판매량")
                st.metric("해당 물량 평균 판매가", f"₩{int(early_adr):,}")
            with c_l2:
                st.metric("⚠️ 누적 기회비용 손실액", f"₩{int(total_lost_revenue):,}", "정상가 방어 시 추가 확보 가능 수익", delta_color="inverse")
                
                st.progress(min(1.0, total_lost_revenue / 100000000))
                
                st.write(f"📢 **결론:** D-14 이전에 적정가 대비 과도하게 할인된 **{int(early_rn):,}실**을 최소한의 마지노선(BAR8 수준)으로만 방어했어도, **₩{int(total_lost_revenue):,}**의 수익을 더 남길 수 있었습니다.")
        else: 
            st.info("객실타입 컬럼을 찾을 수 없어 기회비용 정밀 분석이 불가능합니다.")
            
    st.markdown("---")
    st.subheader("🏟️ 최종 전략 시뮬레이션: GM vs Architect")
    
    base_gross = current_rev_total
    base_rn = current_rn_total
    base_adr = current_adr_actual
    base_net = base_gross - (base_rn * V_C) if 'V_C' in locals() else base_gross - (base_rn * 50000)

    st.markdown("### 🛠️ 단가-물량-수익 상관 시뮬레이션")
    cs1, cs2, cs3 = st.columns(3)
    with cs1: sim_adr = st.number_input("💡 가상 타겟 ADR (원)", min_value=50000, value=int(base_adr) if base_adr > 0 else int(tgt_m['adr']))
    with cs2: sim_rn_pct = st.slider("📉 예상 물량 변동률 (%)", -50, 50, 0)
    with cs3: sim_ota_share = st.slider("💸 OTA 비중 (%)", 0, 100, 70)

    ar_rn = base_rn * (1 + sim_rn_pct / 100)
    ar_gross = sim_adr * ar_rn
    ar_comm = ar_gross * (sim_ota_share / 100) * 0.15
    ar_cost = ar_rn * 50000
    ar_net = ar_gross - ar_comm - ar_cost

    cl, cr = st.columns(2)
    with cl:
        st.subheader("👨‍💼 총지배인 정책 (Current)")
        st.metric("총매출 (Gross)", f"₩{int(base_gross):,}")
        st.error(f"순수익 (Net): ₩{int(base_net):,}")
        st.write(f"가동률: {current_occ_pct:.1f}%")
    with cr:
        st.subheader("🏛️ 아키텍트 전략 (Proposed)")
        gain = ar_net - base_net
        st.metric("가상 총매출 (Gross)", f"₩{int(ar_gross):,}", delta=f"{int(ar_gross - base_gross):+,}")
        st.success(f"가상 순수익 (Net): ₩{int(ar_net):,}")
        st.write(f"순수익 증감: **₩{int(gain):+,}**")

    if gain > 0: st.info(f"💡 **최종 검증:** 가동률을 일부 포기하더라도 단가를 상향하는 것이 순수익 면에서 **₩{int(gain):,}** 더 유리합니다. '채우는 것'이 목표가 아니라 '남기는 것'이 목표여야 합니다.")
    else: st.warning(f"⚠️ **최종 검증:** 현재 설정한 단가와 물량 감소폭으로는 수익 보전이 어렵습니다. 가격 저항선을 다시 확인하십시오.")

    fig_final = px.bar(pd.DataFrame({
        "Strategy": ["GM Policy", "GM Policy", "Architect", "Architect"],
        "Metric": ["Gross Revenue", "Net Profit", "Gross Revenue", "Net Profit"],
        "Amount": [base_gross, base_net, ar_gross, ar_net]
    }), x="Metric", y="Amount", color="Strategy", barmode="group", template="plotly_dark", color_discrete_sequence=['#9e2a2b', '#00D1FF'])
    st.plotly_chart(fig_final, use_container_width=True)

with tabs[8]:
    st.header("🔮 AI 예약 과속 감시")
    if not df_full_pms.empty:
        today_now = datetime.now(timezone(timedelta(hours=9))).replace(tzinfo=None)
        df_r = df_full_pms[df_full_pms['Temp_Bk'] >= (today_now - timedelta(days=7))]
        if not df_r.empty: 
            type_c = find_column(df_full_pms, ['객실타입', 'Room'])
            st.warning("🔥 최근 7일 내 예약이 급증한 일자 리스트입니다. (단가 상향 타겟)")
            ai_df = df_r.groupby(['Stay_Date', type_c])['Daily_RN'].sum().reset_index()
            ai_df.columns = ['투숙일자', '객실타입', '최근 7일 유입 객실수(RN)']
            ai_df['투숙일자'] = ai_df['투숙일자'].dt.strftime('%Y-%m-%d')
            
            styled_ai_df = ai_df.style.format({'최근 7일 유입 객실수(RN)': '{:,.0f}'}).bar(subset=['최근 7일 유입 객실수(RN)'], color='#FF4B4B')
            st.dataframe(styled_ai_df, use_container_width=True, height=350)
    else: st.info("데이터가 없습니다.")

with tabs[9]:
    st.header("🎯 Dynamic Seasonality Price Guide")
    if avail_analysis:
        reco_df = pd.DataFrame(avail_analysis).sort_values(by='velocity', ascending=False)
        def oracle_rec(row):
            if row['velocity'] >= 10: return "🔥 Tier Jump (+2단계)"
            elif row['velocity'] >= 5: return "⚡ Tier Jump (+1단계)"
            return "표준 유지"
        
        reco_df['전략 제안'] = reco_df.apply(oracle_rec, axis=1)
        reco_df.rename(columns={'date': '투숙일자', 'type': '객실타입', 'occ_new': '현재 점유율(%)', 'velocity': '가속도(%p)', 'suggested_tier':'제안 티어'}, inplace=True)
        
        display_cols = ['투숙일자', '객실타입', '현재 점유율(%)', '가속도(%p)', '제안 티어', '전략 제안']
        styled_reco = reco_df[display_cols].style.format({
            '현재 점유율(%)': '{:,.1f}', '가속도(%p)': '{:,.1f}'
        }).map(lambda x: 'background-color: #9e2a2b; color: white; font-weight: bold;' if 'Jump' in str(x) else '', subset=['전략 제안'])
        
        st.dataframe(styled_reco, use_container_width=True, height=400)
        st.plotly_chart(px.density_heatmap(reco_df, x="투숙일자", y="객실타입", z="가속도(%p)", title="Booking Velocity Heatmap", color_continuous_scale="Reds", template="plotly_dark"), use_container_width=True)
    else:
        st.warning("🧐 분석할 재고 데이터가 없습니다.")

# --- 5. 하단 공통 구역 (Advanced Pro-Level Simulator) ---
st.markdown("---")
st.subheader("🔮 Pro-Level Yield Simulator (가격 저항성 기반 정밀 RM 시뮬레이터)")
st.markdown("단가를 올리는 시뮬레이션을 통해, 가격 저항에 따른 예약 이탈률(Churn Rate)을 예측하고 **최종 순수익(Net Revenue) 및 타겟 ADR 방어율**을 도출합니다.")

c1, c2, c3, c4 = st.columns(4)
with c1: sim_type = st.selectbox("🎯 타겟 객실 타입", ['FDB', 'FDE', 'HDP', 'HDT', 'HDF', 'PPV'])
with c2: current_tier = st.selectbox("📉 현재 판매 티어", ['BAR8', 'BAR7', 'BAR6', 'BAR5', 'BAR4', 'BAR3'], index=0)
with c3: target_tier = st.selectbox("📈 목표 상향 티어", ['BAR7', 'BAR6', 'BAR5', 'BAR4', 'BAR3', 'BAR2', 'BAR1'], index=2)
with c4: comp_adr = st.number_input("⚔️ 주변 경쟁사 최저가 (원)", value=380000, step=10000)

c5, c6, c7, c8 = st.columns(4)
with c5: est_rn = st.number_input("📅 타겟 기간 예상 판매 객실수(RN)", value=50, step=5)
with c6: elasticity = st.select_slider("📉 수요 탄력성 (가격 저항)", options=['낮음(비탄력)', '보통', '높음(탄력)'], value='보통')
with c7: st.markdown("<br>", unsafe_allow_html=True); run_sim = st.button("🚀 시뮬레이션 가동", use_container_width=True)

if run_sim:
    cur_price = PRICE_TABLE.get(sim_type, {}).get(current_tier, 0)
    tgt_price = PRICE_TABLE.get(sim_type, {}).get(target_tier, 0)

    if cur_price > 0:
        price_gap_ratio = max(0, (tgt_price - cur_price) / cur_price)
        comp_gap_ratio = max(0, (tgt_price - comp_adr) / comp_adr)

        e_factor = {'낮음(비탄력)': 0.5, '보통': 1.0, '높음(탄력)': 1.8}[elasticity]
        churn_rate = min(1.0, (price_gap_ratio * 0.4 + comp_gap_ratio * 0.6) * e_factor)

        lost_rn = int(est_rn * churn_rate)
        final_rn = est_rn - lost_rn

        cur_rev_sim = cur_price * est_rn
        final_rev = tgt_price * final_rn
        net_gain = final_rev - cur_rev_sim

        st.markdown("#### 📊 Displacement Analysis Report (이탈 객실 vs 최종 수익)")
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("현재 예상 매출 (상향 전)", f"₩{int(cur_rev_sim):,}", f"단가 ₩{cur_price:,}")
        r2.metric("예상 이탈 객실 (Churn)", f"-{lost_rn} RN", f"이탈률 {churn_rate*100:.1f}%", delta_color="inverse")
        r3.metric("최종 판매 예상 (상향 후)", f"{final_rn} RN", f"단가 ₩{tgt_price:,}")
        r4.metric("💰 최종 넷 레비뉴 (Net Gain)", f"₩{int(final_rev):,}", f"{int(net_gain):,} 원")

        if net_gain > 0:
            st.success(f"✅ **[진행 권장]** 객실을 **{lost_rn}개 덜 팔더라도**, 단가 상승분이 볼륨 손실을 압도하여 최종적으로 **+{int(net_gain):,}원**의 추가 이익이 발생합니다. 이는 목표 ADR을 견인하는 핵심 동력이 됩니다.")
        else:
            st.error(f"⚠️ **[진행 보류]** 가격 저항과 경쟁사 단가({comp_adr:,}원)에 밀려, 단가를 올릴 경우 방이 안 팔려 오히려 **{int(net_gain):,}원**의 손실이 발생합니다. 이 구간은 가격 방어선을 유지하세요.")
    else:
        st.error(f"⚠️ {sim_type} 객실의 {current_tier} 가격 정보가 존재하지 않습니다.")
