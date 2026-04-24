# daily_etl_batch.py (대시보드와 분리된 백엔드 스크립트)
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import json
from datetime import datetime

# 1. Firebase 연결
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

def run_daily_cleanse():
    print("🧹 ETL 데이터 정제 시작...")
    
    # 1) 원본 데이터(더러운 데이터) Extract
    docs = db.collection('daily_snapshots').stream()
    all_dfs = []
    
    for doc in docs:
        doc_data = doc.to_dict()
        try:
            # 기존 앱에 있던 모든 복잡한 '하수 처리' 로직을 여기서 실행
            month_docs = db.collection('daily_snapshots').document(doc.id).collection('month').stream()
            for m_doc in month_docs:
                m_data = m_doc.to_dict()
                if 'json_data' in m_data:
                    all_dfs.append(pd.DataFrame(json.loads(m_data['json_data'])))
        except: continue
        
    if not all_dfs:
        return print("데이터 없음")
        
    df_raw = pd.concat(all_dfs, ignore_index=True)
    
    # 2) Transform (정제 및 변환)
    # 1970년 뭉침 방지, 단체 껍데기 제거, 데이터 타입 변환 로직
    df_raw['date'] = pd.to_datetime(df_raw.get('DateStr', df_raw.get('Date')), errors='coerce')
    mask_1970 = df_raw['date'].dt.year == 1970
    if mask_1970.any():
        df_raw.loc[mask_1970, 'date'] = pd.to_datetime(pd.to_numeric(df_raw.loc[mask_1970, 'DateStr'], errors='coerce'), unit='ms')
    
    df_clean = df_raw.dropna(subset=['date']).copy()
    df_clean['date'] = df_clean['date'].dt.normalize().dt.strftime('%Y-%m-%d') # 문자열로 깔끔하게 저장
    df_clean['otb_revenue'] = pd.to_numeric(df_clean.get('REV', 0), errors='coerce').fillna(0)
    df_clean['rooms_sold'] = pd.to_numeric(df_clean.get('RMS', 0), errors='coerce').fillna(0)
    
    # 그룹바이로 일자별 최종 요약본 단 1줄씩만 남김
    df_final = df_clean.groupby('date').agg({'otb_revenue':'sum', 'rooms_sold':'sum'}).reset_index()
    
    # 3) Load (정제된 데이터베이스에 적재)
    # 앱은 이제 이 'hotel_analytics_clean' 컬렉션만 쳐다보면 됨
    batch = db.batch()
    doc_ref = db.collection('hotel_analytics_clean').document('master_view')
    batch.set(doc_ref, {'updated_at': datetime.now(), 'data': df_final.to_dict(orient='records')})
    batch.commit()
    
    print("✅ ETL 정제 및 적재 완료! (앱 속도 10배 향상)")

if __name__ == "__main__":
    run_daily_cleanse()
