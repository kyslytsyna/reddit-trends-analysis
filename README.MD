### Reddit trends project
**Quick exploration of headline engagement in six news-oriented subreddits.**

### Dataset snapshot
* **Source:** Reddit API (`praw`)  
* **Sampling strategy:** first 500 submissions returned by `subreddit.top(all)`  
  – i.e. the 500 highest-scoring posts in each community.  
* **Implications**
  * Date coverage differs by subreddit (posts cluster where engagement peaked).  
  * Time-series plots reflect *high-engagement eras*, **not** true posting volume.  
  * Balanced class sizes (500 × 6 = 3 000 rows) make ML demo fast to train.