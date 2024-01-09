import requests
import apache_beam as beam

class DownloadFromUrl(beam.DoFn):
    def process(self, element):
        """
        :param element: MovieLens Dataset URL
        :type element: str
        :yield: response content
        :rtype: bytes
        """
        response = requests.get(element)
        yield response.content