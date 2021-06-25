import pandas as pd
import numpy as np
import os

train_path = os.path.join('~/s3data/dataset', 'train.csv')
test_path = os.path.join('~/s3data/dataset', 'test.csv')

train = pd.read_csv(train_path)
test = pd.read_csv(test_path)

print(train.head(10))

print(test.head(10))


from sklearn import preprocessing
label = preprocessing.LabelEncoder()

crime = label.fit_transform(train.Category)

train.Dates = pd.to_datetime(train.Dates, format='%Y-%m-%d %H:%M:%S')
test.Dates = pd.to_datetime(test.Dates, format='%Y-%m-%d %H:%M:%S')


days = pd.get_dummies(train.DayOfWeek)
district = pd.get_dummies(train.PdDistrict)
hour = pd.get_dummies(train.Dates.dt.hour)


train_data = pd.concat([days, district, hour], axis=1)
train_data['crime'] = crime


days = pd.get_dummies(test.DayOfWeek)
district = pd.get_dummies(test.PdDistrict)
hour = pd.get_dummies(test.Dates.dt.hour)
test_data = pd.concat([days, district, hour], axis=1)


print(train_data.head(10))
print(test_data.head(10))


from sklearn.model_selection import train_test_split
training, validation = train_test_split(train_data, train_size=0.6)


from sklearn.metrics import log_loss
from sklearn.naive_bayes import BernoulliNB

model = BernoulliNB()
feature_list = training.columns.tolist()
feature_list = feature_list[:len(feature_list) - 1]
print('选取的特征列：', feature_list)
model.fit(training[feature_list], training['crime'])

predicted = np.array(model.predict_proba(validation[feature_list]))
print("朴素贝叶斯log损失为 %f" % (log_loss(validation['crime'], predicted)))


from sklearn.linear_model import LogisticRegression
model = LogisticRegression(C=0.1, solver='sag')
model.fit(training[feature_list], training['crime'])

predicted = np.array(model.predict_proba(validation[feature_list]))
print("逻辑回归log损失为 %f" %(log_loss(validation['crime'], predicted)))


from sklearn.ensemble import RandomForestClassifier

params = [12,13,14,15,16]
for par in params:
    clf = RandomForestClassifier(n_estimators=30, max_depth=par)
    clf.fit(training[feature_list], training['crime'])
    predicted = np.array(clf.predict_proba(validation[feature_list]))
    print("随机森林log损失为 %f" %(log_loss(validation['crime'], predicted)))

test_predicted = np.array(model.predict_proba(test_data[feature_list]))


col_names = np.sort(train['Category'].unique())
print(col_names)
result = pd.DataFrame(data=test_predicted, columns=col_names)
result['Id'] = test['Id'].astype(int)
result.to_csv('output.csv', index=False)