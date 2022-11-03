import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import preprocessing
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from matplotlib import pyplot as plt
import seaborn as sns

df = pd.read_csv("data/2022_10_26_all_tweets_extended.csv")
df = df.iloc[:,1:]

df.drop(['author_id', 'text', 'hashtags', 'created_at', 'mined_at', 'like_count',
    'quote_count', 'reply_count', 'retweet_count', 'like_binary', 'retweet_binary'], axis=1, inplace=True)

def logistic_ml():
    X = df.iloc[:,df.columns != 'reply_binary']
    y = df.reply_binary

    X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, random_state=5, stratify=y)

    scaler = preprocessing.StandardScaler().fit(X_train)
    X_train_scaled = scaler.transform(X_train)

    X_test_scaled = scaler.transform(X_test)

    import matplotlib.colors as mcolors
    colors = list(mcolors.CSS4_COLORS.keys())[10:]

    def draw_histograms(dataframe, features, rows, cols):
        fig=plt.figure(figsize=(20,20))
        for i, feature in enumerate(features):
            ax=fig.add_subplot(rows,cols,i+1)
            dataframe[feature].hist(bins=20,ax=ax,facecolor=colors[i])
            ax.set_title(feature+" Histogram",color=colors[35])
            ax.set_yscale('log')
        fig.tight_layout() 
        plt.savefig('Histograms.png')
        plt.show()
    # draw_histograms(df,df.columns,8,4)

    model = LogisticRegression()
    model.fit(X_train_scaled, y_train)
    y_pred = model.predict(X_test_scaled)

    train_acc = model.score(X_train_scaled, y_train)
    # print("The Accuracy for Training Set is {}".format(train_acc*100))

    test_acc = accuracy_score(y_test, y_pred)
    print("The Accuracy for Test Set is {}".format(test_acc*100))

    cm=confusion_matrix(y_test,y_pred)
    plt.figure(figsize=(12,6))
    plt.title("Confusion Matrix")
    sns.heatmap(cm, annot=True,fmt='d', cmap='Blues')
    plt.ylabel("Actual Values")
    plt.xlabel("Predicted Values")
    plt.savefig('confusion_matrix.png')

