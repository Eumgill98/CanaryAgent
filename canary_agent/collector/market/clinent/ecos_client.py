import os
import requests
import pandas as pd
from typing import Optional

class ECOSClient:
    BASE_URL = "https://ecos.bok.or.kr/api/StatisticSearch"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("ECOS_API_KEY")
        if not self.api_key:
            raise EnvironmentError("ECOS_API_KEY is not set")
    
    def get_series(self, 
                   stat_code: str,
                   item_code1: str,
                   item_code2: str = "",
                   item_code3: str = "",
                   start: str = None, 
                   end: str = None,
                   cycle: str = "M") -> pd.DataFrame:
        """
        한국은행 ECOS API에서 통계 데이터를 가져옵니다.
        
        Args:
            stat_code: 통계표 코드 (예: "200Y001" - 주요통계)
            item_code1: 통계항목코드1 (예: "10101" - 국내총생산)
            item_code2: 통계항목코드2
            item_code3: 통계항목코드3
            start: 시작일 (YYYY-MM 형식)
            end: 종료일 (YYYY-MM 형식)
            cycle: 주기 (M=월, Q=분기, Y=년)
        """
        # 날짜 형식 변환
        start_date = start.replace("-", "") if start else "190001"
        end_date = end.replace("-", "") if end else "209912"
        
        # URL 구성: /서비스명/인증키/요청유형/언어/시작번호/종료번호/통계표코드/주기/시작일자/종료일자/통계항목코드1/통계항목코드2/통계항목코드3
        url = f"{self.BASE_URL}/{self.api_key}/json/kr/1/10000/{stat_code}/{cycle}/{start_date}/{end_date}/{item_code1}/{item_code2}/{item_code3}"
        
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            
            json_data = resp.json()
            
            # 에러 체크
            if "RESULT" in json_data:
                result = json_data["RESULT"]
                if result.get("CODE") != "INFO-000":
                    raise ValueError(f"API Error: {result.get('MESSAGE')}")
            
            # 데이터 추출
            data = json_data.get("StatisticSearch", {}).get("row", [])
            
            if not data:
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            
            # 날짜 처리 (주기에 따라 다르게)
            if cycle == "M":
                df["date"] = pd.to_datetime(df["TIME"], format="%Y%m")
            elif cycle == "Q":
                df["date"] = pd.to_datetime(df["TIME"], format="%Y%m")
            elif cycle == "Y":
                df["date"] = pd.to_datetime(df["TIME"], format="%Y")
            
            # 데이터 값 변환
            df["value"] = pd.to_numeric(df["DATA_VALUE"], errors="coerce")
            
            # 필요한 컬럼만 선택
            result_df = df[["date", "value"]].copy()
            result_df = result_df.sort_values("date").reset_index(drop=True)
            
            return result_df
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"API 요청 실패: {e}")
        except KeyError as e:
            raise ValueError(f"응답 데이터 파싱 실패: {e}")