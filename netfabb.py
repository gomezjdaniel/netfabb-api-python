
import os
import hashlib

import requests
import xmltodict


'''
  Exceptions
'''
class NetfabbCantConnectToAPI(Exception): pass
class NetfabbErrorNoError(Exception): pass
class NetfabbResultMissing(Exception): pass
class NetfabbXMLResponseError(Exception): pass


'''
  Netfabb API client
'''
class Netfabb(object):
  def __init__(self, login, password):
    self._login = login
    self._password = password

    self._base_url = 'https://netfabb.azurewebsites.net/api/'

  def _get_base_parameters(self):
    parameters = {}

    parameters['login'] = self._login
    parameters['password'] = self._password
    parameters['protocol'] = 100

    return parameters

  def _call_method(self, method_name, parameters=None):
    if parameters == None:
      parameters = {}

    parameters.update(self._get_base_parameters())
    parameters['methodname'] = method_name

    if 'filename' in parameters:
      files = { 
        'filedata': (os.path.basename(parameters['filename']), open(parameters['filename'], 'rb'), 
            'application/octet-stream')
      }

      parameters.pop('filename', None)

      response = requests.post(url=self._base_url, data=parameters, files=files, verify=True)
    else:
      response = requests.post(url=self._base_url, params=parameters, verify=True)

    if response.status_code != 200:
      raise NetfabbCantConnectToAPI(response)

    output_obj = (xmltodict.parse(response.content))['netfabbapi']

    if int(output_obj['success']) == 1:
      return output_obj
    elif not output_obj['errorcode'] or not output_obj['errormessage']:
      raise NetfabbXMLResponseError
    else:
      raise NetfabbErrorNoError

  def new_project(self):
    output = self._call_method('newproject')

    if int(output['success']) == 1:
      if not output['projectuuid']:
        raise NetfabbResultMissing

      return output['projectuuid']    

    raise NetfabbErrorNoError

  def file_upload(self, project_id, file_name, description=None):
    if description == None:
      description = 'No description provided'

    file_name = os.path.realpath(file_name)
    if not os.path.isfile(file_name):
      raise NetfabbFileError

    file_size = os.stat(file_name).st_size
    if file_size <= 0 or file_size > 104857600:
      raise NetfabbFileSizeError

    file_md5 = hashlib.md5(open(file_name).read()).hexdigest()
    if not file_md5:
      raise NetfabbFileError

    parameters = {
      'projectuuid': project_id,
      'description': description,
      'filesize': file_size,
      'filemd5': file_md5,
      'filename': file_name
    }

    output = self._call_method('uploadfile', parameters)

    if int(output['success']) == 1:
      if not output['fileuuid']:
        raise NetfabbResultMissing

      return output['fileuuid']    

    raise NetfabbErrorNoError

  def new_job(self, project_id, job_type, job_parameters):
    parameters = {
      'projectuuid': project_id,
      'jobtype': job_type,
      'jobparameter': job_parameters
    }

    output = self._call_method('newjob', parameters)

    if int(output['success']) == 1:
      if not output['jobuuid']:
        raise NetfabbResultMissing

      return output['jobuuid']    

    raise NetfabbErrorNoError

  def retrieve_job_status(self, job_id):
    parameters = {
      'jobuuid': job_id
    }

    output = self._call_method('retrievejobstatus', parameters)

    if int(output['success']) == 1:
      if not output['jobstatus']:
        raise NetfabbResultMissing

      return output['jobstatus']    

    raise NetfabbErrorNoError

  def retrieve_job_results(self, job_id):
    parameters = {
      'jobuuid': job_id
    }

    output = self._call_method('retrievejobresults', parameters)

    return output

  def file_download(self, file_id, file_name):
    parameters = self._get_base_parameters()
    parameters['methodname'] = 'downloadfile'
    parameters['fileuuid'] = file_id

    response = requests.get(self._base_url, params=parameters, verify=True, stream=True)
    if response.status_code != 200:
      raise NetfabbCantConnectToAPI(response)

    with open(file_name, 'wb') as f:
      for chunk in response.iter_content(chunk_size=1024): 
        if chunk: 
          f.write(chunk)
          f.flush()

    return True
    
