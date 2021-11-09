import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os


def main():
    input_file = 'pipeline_data/input/el_quijote.txt'
    output_file = 'pipeline_data/output/'
    run_pipeline_2(input_file)
    files = os.listdir('.')
    filename = ''
    for file in files:
        if 'number' in file:
            filename = file
            break
    number = int(open(filename, 'r').read())
    run_pipeline_asc(input_file, output_file, number)
    run_pipeline_desc(input_file, output_file, number)

def healthy_text(word):
    to_remove = [',', '.', '-', ':', ' ', "'", '"']
    for symbol in to_remove:
        word = word.replace(symbol,'')

    word = word.lower()
    word = word.replace('á','a')
    word = word.replace('é','e')
    word = word.replace('é','i')
    word = word.replace('ó','o')
    word = word.replace('ú','u')
    return word

def run_pipeline_asc(input_file, output_file, shape):
    with beam.Pipeline(options = PipelineOptions()) as p:
        words = read_data_and_split(p,input_file) 
        clean_words = words | beam.Map(healthy_text)
        count_words = clean_words | beam.combiners.Count.PerElement()
        ascending_sort = count_words | beam.combiners.Top.Of(shape,key = lambda x: x[1])
        ascending_sort | beam.io.WriteToText(f'{output_file}ascend', file_name_suffix='.txt')

def run_pipeline_desc(input_file, output_file, shape):
    with beam.Pipeline(options = PipelineOptions()) as p:
        words = read_data_and_split(p,input_file) 
        clean_words = words | beam.Map(healthy_text)
        count_words = clean_words | beam.combiners.Count.PerElement()
        descending_sort = count_words | beam.combiners.Top.Of(shape,key = lambda x: x[1], reverse = True)
        descending_sort | beam.io.WriteToText(f'{output_file}descend', file_name_suffix='.txt')

def run_pipeline_2(input_file):
    with beam.Pipeline(options=PipelineOptions()) as pipeline2:
        words =  read_data_and_split(pipeline2,input_file)
        count_words = words | beam.combiners.Count.PerElement()
        total_tuples = count_words | beam.combiners.Count.Globally() 
        total_tuples | beam.io.WriteToText('number', file_name_suffix='.txt')

def read_data_and_split(pipeline,input_file):
    lines = pipeline | beam.io.ReadFromText(input_file)
    words = lines | beam.FlatMap(lambda line: line.split())
    return words

if __name__ == '__main__':
    main()