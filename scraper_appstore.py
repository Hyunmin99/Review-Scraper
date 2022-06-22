import pandas as pd
from app_store_scraper import AppStore
import os
import numpy as np

def AppStoreReview(Name, iosName, iosId) :
    def getBody(data) :
        if type(data) == dict :
            return data["body"]
        else :
            return np.nan
    
    app = AppStore(country='kr', app_name=iosName.encode('utf-8'), app_id = iosId)

    app.review(sleep = 25)
    
    df_result = pd.DataFrame(app.reviews)
    
    if "developerResponse" not in df_result.columns :
        df_result["developerResponse"] = np.nan
    
    df_result = df_result[["title", "review", "rating", "date", "developerResponse"]].reset_index(drop=True)
    df_result["developerResponse"] = df_result["developerResponse"].map(lambda x: getBody(x))
    df_result['at'] = df_result['at'].apply(lambda x:str(x))
    pre_file = pd.read_csv(f'total/total_review_appstore_{Name}.csv', encoding='utf-8-sig') if os.path.isfile(f'total/total_review_appstore_{Name}.csv') else pd.DataFrame()
    file = pd.concat([pre_file, df_result]).drop_duplicates(keep = 'first')
    
    count = len(file) - len(pre_file)
    print(f'{count}개의 리뷰가 추가 되었습니다!')
    #전체 리뷰 저장
    file.to_csv(f'total/total_review_appstore_{Name}.csv', encoding='utf-8-sig', index=False)
    #1점 리뷰 저장
    file[file["rating"] == 1 ].to_csv(f'score_one/one_review_appstore_{Name}.csv', encoding='utf-8-sig', index=False)
    
    print(f'App Store {Name} 전체 리뷰, 1점 리뷰 저장!')
    print("-")

list = pd.read_csv("0.crawling_list.csv")

list.apply( lambda x: AppStoreReview(x['Name'], x["iosName"], x["iosId"]), axis = 1)
crawl_list = list["Name"].to_list()

print(f'*************App Store {crawl_list} 리뷰 데이터 크롤링 완료!*************')