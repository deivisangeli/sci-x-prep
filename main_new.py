import os
import pandas as pd
import numpy as np
import pathlib
import matplotlib.pyplot as plt
from pathlib import Path
#from juntator import aggregate_all
from get_relevant_works import get_all
from make_work_dataset import prep_works, agg_relevant_works
from get_citations_for_each_work import set_aside_citations, agg_citations, count_citations_per_author_per_year
from sample_authors import get_all_authors, aggregate_authors




if __name__ == "__main__":
    # Local Directories definitions-------------------------------------------------------------------------------------------
    potentially_on_twitter_path = Path(os.getenv('db_path')) / "Science Twitter/Data/Raw/openalex/people/VS_academics_2025-01/allAcademics.csv"
    potentially_on_twitter_filename = Path(potentially_on_twitter_path).stem.replace(
        ".csv", ""
    )  # File name without extension

    relevant_dir = Path("data/relevant_works")  # <<< where the set-aside works will be saved

    # Ensure directories exist-------------------------------------------------------------------------------------------------
    relevant_dir.mkdir(parents=True, exist_ok=True)
    works_csvs_dir = Path("data/works_csvs")
    works_csvs_dir.mkdir(parents=True, exist_ok=True) 
    #aggregated_dir.mkdir(parents=True, exist_ok=True)



    # Logging configuration---------------------------------------------------------------------------------------------------
    log_file = "process_log.log"

    # PROCESSING PIPELINE------------------------------------------------------------------------------------------------------

    # # Get all authors
    # get_all_authors(output_dir=Path("data/selected_authors"))

    # Aggregate all authors
    # aggregate_authors(input_dir=Path("data/selected_authors"), output_file=Path("data/selected_authors") / "all_data.csv")

    # # there are too many, over 5m, so we need to sample them. Let's pick 75k
    # all_authors= pd.read_csv(Path("data/selected_authors") / "all_data.csv")
    # np.random.seed(42)
    # sample_authors = all_authors.sample(n=75000) 
    # sample_authors.to_csv(Path("data/selected_authors") / "sample_authors_2025-03-16.csv", index=False)
    # # add to these authors those in the potential mathches file. They will be our target authors
    # sample_authors = pd.read_csv(Path("data/selected_authors") / "sample_authors_2025-03-16.csv")
    # n_potentially_on_twitter = pd.read_csv(potentially_on_twitter_path).shape[0]
    # potential_matches = pd.read_csv(potentially_on_twitter_path)['id']
    # sample_authors = pd.concat([sample_authors, potential_matches])
    # # drop duplicates
    # sample_authors = sample_authors.drop_duplicates()
    # # how many authors do we have now?
    # print( len(sample_authors))
    # # how many dups were dropped?
    # print(n_potentially_on_twitter+75000 - len(sample_authors)) # 670, makes sense I guess since it's way more people than the 75k
    # # save the sample authors
    # sample_authors.to_csv(Path("data/selected_authors") / "sample_authors_2025-03-16.csv", index=False)

    # # Let's first put aside the relevant works
    # get_all(
    #     valid_ids_path = Path("data/selected_authors") / "sample_authors_2025-03-16.csv", 
    #     output_dir=relevant_dir, 
    #     id_col='id'
    # )

    # bestMathcIdsPath = "C:/Users/deivi/Dropbox/Science Twitter/Data/Intermediate/VS_academics_bestMatch.csv"
    # bestMatchIds = "OA_id"  # <<< INPUT
    # bestMathcIdsFilename = Path(bestMathcIdsPath).stem.replace(".csv", "")  

    # # get the data we want to extract from each work and put it into csvs. Then we will aggregate them
    # prep_works(
    #     valid_ids_path = Path("data/selected_authors") / "sample_authors_2025-03-16.csv",
    #     input_dir =  relevant_dir / "sample_authors_2025-03-16",
    #     output_dir = works_csvs_dir,
    # )

    # # put works in a single dataset
    # agg_relevant_works(
    #     input_dir= works_csvs_dir / "sample_authors_2025-03-16",
    #     output_dir = works_csvs_dir / "sample_authors_2025-03-16",
    # )

    # # have a look at the data integrity in the aggregated dataset
    # data= pd.read_csv(works_csvs_dir / "sample_authors_2025-03-16" / "sample_authors_2025-03-16_all_data.csv")
    # print(data.head())
    # print(data.shape)
    # print(data.columns)
    # print(data.dtypes)
    # print(data.isnull().sum()/data.shape[0]) 
    # # make a histogram of the publication years
    # plt.hist(data['year'], bins=range(1900, 2026))
    # plt.show()

    # STOPPED HERE ON 2025-03-16

    # set aside the citations to those works
    
    # set_aside_citations(
    #     output_dir = Path("data/citing_works"),
    #     work_ids_path = Path(selected_dir) / bestMathcIdsFilename/ "all_data.csv",
    #     sample_name = bestMathcIdsFilename,
    #     test=False
    # )

    # agg_citations
    # agg_citations(
    #     input_dir = Path("data/citing_works") / bestMathcIdsFilename,
    #     output_dir = Path("data/citing_works") / bestMathcIdsFilename,
    #     work_ids_path = Path(selected_dir) / bestMathcIdsFilename/ "all_data.csv",
    # )

    # # count_citations_per_author_per_year
    # count_citations_per_author_per_year(
    #     agg_citations= Path("data/citing_works") / bestMathcIdsFilename / "all_data.csv",
    #     relelant_ids_path = bestMathcIdsPath,
    #     relevant_ids_column = "OA_id",
    #     output_dir = Path("data/citing_works") / bestMathcIdsFilename,
    #     test=False
    # )



