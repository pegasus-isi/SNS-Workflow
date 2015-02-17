import numpy
import ast
from Pegasus.DAX3 import *

__ALL__ = ["KegParametersFactory"]

class KegParametersFactory:
	keg_parameters = {
		"cpu_time": "-T",
		"wall_time": "-t",
		"memory": "-m"
	}

	def __init__(self, config):
		self.config = config
		numpy.random.seed()

	def output_file(self, task, filename, file_real_path=""):
		if not file_real_path:
			file_real_path = filename

		if not self.config.has_option("keg-%s" % task, filename):
			# print "We have not found option ", "keg-%s" % task, filename
			return "-o {0}".format(file_real_path)

		output_params = ast.literal_eval(self.config.get("keg-%s" % task, filename))
		distribution = getattr(numpy.random, output_params['distribution'])
		random_value = int(round( distribution(*output_params['dist_params']) ))

		return "-o {filename}={filesize}{size_unit}".format(filename=file_real_path, filesize=random_value, 
			size_unit=output_params['size_unit'])

	def generate_input_file(self, file_label, filepath):
		if self.config.has_option("keg-input-files", file_label):
			input_file_params = ast.literal_eval(self.config.get("keg-input-files", file_label))
			distribution = getattr(numpy.random, input_file_params['distribution'])
			random_size = int(round( distribution(*input_file_params['dist_params']) )) 
			size_units = { "B": 1, "K": 1024, "M": 1024*1024, "G": 1024*1024*1024  }

			print "Writing file with random size ", filepath

			f = open(filepath,"wb")
			f.seek(random_size * size_units[input_file_params['size_unit']] - 1)
			f.write("\0")
			f.close()
		else:
			print "Writing file with concrete size ", filepath

			f = open(filepath,"wb")
			f.seek(1024 - 1)
			f.write("\0")
			f.close()

	def performance_attr(self, task, param):
		if not self.config.has_option("keg-%s" % task, param):
			return ""

		param_dict = ast.literal_eval(self.config.get("keg-%s" % task, param))
		param_value = int(round( getattr(numpy.random, param_dict['distribution'])(*param_dict['dist_params']) ))

		return "{0} {1}".format(KegParametersFactory.keg_parameters[param], param_value)

	def other_params(self, task):
		if not self.config.has_option("keg-%s" % task, "other_params"):
			return ""

		return self.config.get("keg-%s" % task, "other_params")

	def add_keg_params(self, job, job_label=""):
	    """Generates pegasus(-mpi)-keg parameters based on the job object and config file:
	    - output files: based on Job linking info
	    - performance attributes: cpu_time, wall_time, memory
	    - other parameters (anything you want)
	    """

	    if not job_label:
	        job_label = job.node_label

	    # Adding output files based on links defined in a job
	    # for output_file in filter( (lambda x: x.link == Link.OUTPUT), job.used ):
	    #     print "Job label:", job_label, "\t Output file name:", output_file.name
	    #     job.addArguments(self.output_file( job_label, output_file.name ))

	    for performance_parameter in [ "cpu_time", "wall_time" ]:
	        job.addArguments(self.performance_attr( job_label, performance_parameter ))

	    job.addArguments(self.other_params(job_label))		
