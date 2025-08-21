# reddit-trends-project

Exploratory data analysis of reddit’s top posts from a few selected subreddits.  
This project is not about building ML model, it was just for practicing data cleaning and EDA, plus a bit of text analysis.  

---

## Project overview

- Dataset: around **3 000 reddit posts** from six subreddits:  
  `news`, `worldnews`, `politics`, `technology`, `worldpolitics`, `TrueReddit`
- Balanced sample, ~500 posts each
- Data is only **top scoring posts**, so more about trends than about total volume
- I also wrote a small **SQL file** with some EDA queries, just to practice

---

## What I did

1. Prepared the data (dates parsing, restored tokens lists from csv)  
2. Looked at distribution of posts by year and month  
3. Checked **average score** and **average comments** per subreddit  
4. Compared **score vs comments** (log-log scatter)  
5. Did some **text analysis** – word frequency, wordclouds  
6. Title length by subreddit (boxplots)  
7. Sentiment analysis with TextBlob  
8. Posting hour heatmap (median score by UTC hour)

---

## Main insights

- Sampling worked well, ~500 posts per subreddit, so comparison is fair  
- **Scores and comments are correlated**, but not 1:1 (doubling score doesn’t double comments)  
- `news`, `worldnews` and `politics` have the **highest engagement**  
- `technology` posts are episodic, big peaks when hype cycle (AI, platforms, etc)  
- `TrueReddit` has **longer titles** (~20 words) but much lower scores  
- Sentiment of headlines is almost neutral everywhere → tone doesn’t really matter  
- Posting hour has some effect, but not huge compared to subreddit size/topic

---

## Tools

- Python: pandas, matplotlib, seaborn, wordcloud, textblob  
- Jupyter notebook  
- SQL (just simple queries for practice)

---

## Notes

This is a **learning project**.  
Dataset is limited (only top scoring posts), so results are not real representation of reddit activity.  
The main purpose was to practice wrangling, visualisation and writing down some insights.  
