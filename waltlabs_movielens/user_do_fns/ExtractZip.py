from io import BytesIO
import zipfile

import apache_beam as beam

class ExtractZip(beam.DoFn):
    def process(self, element):
        """
        :param element: Movielens dataset byte object
        :type element: byte
        :yield: filename, and content of file
        :rtype: tuple(str, str)
        """
        zip_data = BytesIO(element)
        with zipfile.ZipFile(zip_data) as zip_file:
            for file_info in zip_file.infolist():
                with zip_file.open(file_info) as file:
                    if file_info.filename.endswith('.csv'):
                        yield file_info.filename, file.read()