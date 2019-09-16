#########################
SocialSent Sentiment Data
#########################

This directory contains sentiment lexicons for the top 250 subreddits on Reddit (by comment count excluding non-English communities)
See http://nlp.stanford.edu/projects/socialsent for links to the accompanying paper, with details on the algorithm, seeds words, and data sources.

All files are .tsv's of the form:

<word> <mean_sentiment> <std_sentiment>

where mean_sentiment is the averaged inferred sentiment across bootstrap-sampled SentProp runs 
and std_sentiment is the standard deviation of these samples.

SentProp was run with the following hyperparameters:

num nearest neighbors k=25
random walk beta=0.9
50 bootstrap samples of size 7
