from sdgym.synthesizers import IndependentSynthesizer, UniformSynthesizer
from sdgym.evaluate import evaluate

def benchIndependentSynthesizer():
    data = np.loadtxt('down_data.csv', delimiter=',', skiprows=1)

    with open('generated_metadata.json') as data_file:
        data2 = json.load(data_file)

    categorical_columns = list()
    ordinal_columns = list()

    for column_idx, column in enumerate(data2['columns']):

        if column['type'] == CATEGORICAL:
            print(column)
            print('Classified as Categorical')
            categorical_columns.append(column_idx)
        elif column['type'] == ORDINAL:
            ordinal_columns.append(column_idx)
            print(column)
            print('Classified as Ordinal')

    synthesizer = IndependentSynthesizer()
    synthesizer.fit(data, categorical_columns, ordinal_columns)

    sampled = synthesizer.sample(300)
    scores = benchmark(synthesizer.fit_sample)
    scores = scores.append(synthesizer.fit_sample)
    scores = scores.append(synthesizer.fit_sample)
    print('\nEvaluation Scores from evaluate function:\n')
    print (scores)
    scores['Synth'] = 'IndependentSynthesizer'
    scores.to_csv('IndependentBench.csv')

benchIndependentSynthesizer()