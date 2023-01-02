import sent2vec
from nltk import word_tokenize
from nltk.corpus import stopwords
from string import punctuation
from scipy.spatial import distance


model_path = '/Users/aaron/Downloads/BioSentVec_PubMed_MIMICIII-bigram_d700.bin'
model = sent2vec.Sent2vecModel()
try:
    model.load_model(model_path)
except Exception as e:
    print(e)
print('model successfully loaded')


stop_words = set(stopwords.words('english'))
def preprocess_sentence(text):
    text = text.replace('/', ' / ')
    text = text.replace('.-', ' .- ')
    text = text.replace('.', ' . ')
    text = text.replace('\'', ' \' ')
    text = text.lower()

    tokens = [token for token in word_tokenize(text) if token not in punctuation and token not in stop_words]

    return ' '.join(tokens)

sentence_vector1 = model.embed_sentence(preprocess_sentence('UMIAMI MEDICINE - UROLOGY'))
sentence_vector2 = model.embed_sentence(preprocess_sentence('UMIAMI MEDICINE - NEUROLOGY'))

cosine_sim = 1 - distance.cosine(sentence_vector1, sentence_vector2)
print('cosine similarity:', cosine_sim)