#!/usr/bin/python

"""*
 * Copyright 2009 University of Victoria
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * AUTHOR - Adam Bishop - ahbishop@uvic.ca
 *
 * For comments or questions please contact the above e-mail address
 * or Ian Gable - igable@uvic.ca
 *
 * """


import logging

class Logger(object):
    """ A class to encapsulate useful logging features and setup

    """
    def __init__(self, name, errorLogFile):


        self.logger = logging.getLogger(name)

        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')

        debug_handler = logging.StreamHandler()
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(formatter)

        file_output_handlr = logging.FileHandler(errorLogFile)
        file_output_handlr.setFormatter(formatter)
        file_output_handlr.setLevel(logging.INFO)

        self.logger.addHandler(file_output_handlr)
        self.logger.addHandler(debug_handler)

    def warning(self, msg):
        self.logger.warning(msg)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def debug(self, msg):
        self.logger.debug(msg)

