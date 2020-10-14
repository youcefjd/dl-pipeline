import fastai
from fastai.text.all import *
from sqlalchemy import create_engine
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import getpass

def processing():
    #Model Set Up and Loading 
    pth = getpass.getpass(str('Please input the S3 path to your file: '))
    tableName = input(str('Please input the table name to input your predictions: '))
    user = input(str('Please input your DB username: '))
    password = getpass.getpass(str('Please input your DB password: '))
    bats = input(int('Please input the batch size for processing: '))
    bs = 128
    path = untar_data(URLs.IMDB_SAMPLE)
    df = pd.read_csv(path/'texts.csv')
    imdb_lm = DataBlock(blocks=(TextBlock.from_df('text', is_lm=True),),
                    get_x=ColReader('text'),
                    splitter=RandomSplitter())
    
    dbunch_lm = imdb_lm.dataloaders(df)
    path = untar_data(URLs.IMDB)
    imdb_lm = DataBlock(blocks=(TextBlock.from_folder(path, is_lm=True),),
                    get_items=partial(get_text_files, folders=['train', 'test', 'unsup']),
                    splitter=RandomSplitter(0.1))
    dbunch_lm = imdb_lm.dataloaders(path, path=path, bs=bs, seq_len=80)
    learn = language_model_learner(dbunch_lm, AWD_LSTM, drop_mult=0.3, metrics=[accuracy, Perplexity()])
    learn.load('/usr/local/spark/models1/fine_tuned')
    def read_tokenized_file(f): return L(f.read().split(' '))
    imdb_clas = DataBlock(blocks=(TextBlock.from_folder(path, vocab=dbunch_lm.vocab),CategoryBlock),
                          get_x=read_tokenized_file,
                          get_y = parent_label,
                          get_items=partial(get_text_files, folders=['train', 'test']),
                          splitter=GrandparentSplitter(valid_name='test'))
    dbunch_clas = imdb_clas.dataloaders(path, path=path, bs=bs, seq_len=80)
    learn = text_classifier_learner(dbunch_clas, AWD_LSTM, drop_mult=0.5, metrics=accuracy)
    learn.load_encoder('/usr/local/spark/models1/fine_tuned_enc')
    learn.load('/usr/local/spark/models1/fine_tuned_classifier_no16')
    #Loading and Processing Data
    data = pth
    data = [json.loads(line) for line in open(data)]
    df = pd.DataFrame(data)
    text = [i for i in df.text]
    df = pd.DataFrame(text)
    preds = [learn.predict(i) for i in df.s.head(bats)]
    preds = pd.DataFrame(preds)
    preds = preds.rename(columns={0: "sentiment", 1: "category", 2: "confidence"})
    engine = create_engine('postgresql://' +user+ ':' +password+'@10.0.0.4:5432/'+database)
    preds['sentiment'].to_sql(tableName, engine)
    return 