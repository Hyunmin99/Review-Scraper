from google_play_scraper import Sort, reviews_all
import pandas as pd
import os

def PlayStoreReview(Name, aosId) :
    result = reviews_all(
        aosId,
        sleep_milliseconds=1000, # defaults to 0
        lang='ko', # defaults to 'en'
        country='us', # defaults to 'us'
        sort=Sort.MOST_RELEVANT, # defaults to Sort.MOST_RELEVANT
    #     filter_score_with=5 # defaults to None(means all score) 
    )

    df_result = pd.DataFrame(result)
    df_result = df_result[["reviewId", "content", "score", "at", "replyContent"]].reset_index(drop=True)
    df_result['at'] = df_result['at'].apply(lambda x:str(x))

    pre_file = pd.read_csv(f'total/total_review_playstore_{Name}.csv') if os.path.isfile(f'total/total_review_playstore_{Name}.csv') else pd.DataFrame()
    file = pd.concat([pre_file, df_result]).drop_duplicates(subset = ["reviewId", "content", "score", "at", "replyContent"])
    
    count = len(file) - len(pre_file)
    print(f'{count}개의 리뷰가 추가 되었습니다!')
    #전체 리뷰 저장
    file.to_csv(f'total/total_review_playstore_{Name}.csv', encoding='utf-8-sig', index=False)

    # 1점 리뷰 저장
    file[file["score"] == 1 ].to_csv(f'score_one/one_review_playstore_{Name}.csv', encoding='utf-8-sig', index=False)
    
    print(f'Play Store - {Name} 전체 리뷰, 1점 리뷰 저장!')
    print('-')

list = pd.read_csv("0.crawling_list.csv")

list.apply( lambda x: PlayStoreReview(x['Name'], x["aosId"]), axis = 1)
crawl_list = list["Name"].to_list()

print(f'*************Play Store {crawl_list} 리뷰 데이터 크롤링 완료!*************')