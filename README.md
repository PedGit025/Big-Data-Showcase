# What

This is a project for Handling, Processing Big Data and Generate Insights from Big Data

# Process

1. Get Movie Details from [TMDB](https://www.themoviedb.org/) (API)

2. Data Processing - Cleaning Data, formatting and removing stop words (NLTK module) 

3. Store in a Data warehouse (AWS and local machine) and utilize Online analytical processing (OLAP) and simulate Online Transaction Processing (OLTP) through S3 to local using the [Updater](https://github.com/PedGit025/Big-Data-Showcase/blob/main/Update%20Scrape%20progress.ipynb)

4. Analyze data and Generate Dashboard via [tableau](https://public.tableau.com/app/profile/lizelle.cruz/viz/IMDBDashboard_17223681730490/IMDBDashboard?publish=yes)


# Files

1. Data Analysis.pdf - Data Analysis presentation

2. IMDB Dashboard.png - Sample Image of Dashboard

3. PreprocessF.ipynb - Preprocessing of the scraped movie details using Pandas

4. SPARK, SQL, EMR.py - Using AWS EMR to run SPARK and SQL commands for Big Data Analysis

5. Scrape IMDB through API.ipynb - Get Movie Details as dataset 'Title', 'Year', 'Revenue', 'Budget', 'Runtime', 'Actors', 'Rating', 'Production_company', 'Genre', and 'IMDb_code'

6. Update Scrape progress.ipynb - This code checks if there are new movies added to IMDB and it adds it to the dataset, using pickle module it loads the current data then runs the scraping to check for updates to simulate Online Transaction Processing (OLTP)

7. Use Python data cleaning in AWS s3.ipynb - Utilized AWS s3 and CLI to remove **stop** words from the dataset before processing
   
9. data warehousing.ipynb - Process the cleaned data to Star Schema

10. main.py - Using AWS EMR to use spark and SQL commands to generate insights from the data




# Co Creators

lizelle_ann_v_cruz@dlsu.edu.ph

gerald_doblado@dlsu.edu.ph

ruz_valdez@dlsu.edu.ph

