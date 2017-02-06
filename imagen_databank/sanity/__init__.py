# -*- coding: utf-8 -*-
# noqa

# Copyright (c) 2014-2017 CEA
#
# This software is governed by the CeCILL license under French law and
# abiding by the rules of distribution of free software. You can use,
# modify and/ or redistribute the software under the terms of the CeCILL
# license as circulated by CEA, CNRS and INRIA at the following URL
# "http://www.cecill.info".
#
# As a counterpart to the access to the source code and rights to copy,
# modify and redistribute granted by the license, users are provided only
# with a limited warranty and the software's author, the holder of the
# economic rights, and the successive licensors have only limited
# liability.
#
# In this respect, the user's attention is drawn to the risks associated
# with loading, using, modifying and/or developing or reproducing the
# software by the user in light of its specific status of free software,
# that may mean that it is complicated to manipulate, and that also
# therefore means that it is reserved for developers and experienced
# professionals having in-depth computer knowledge. Users are therefore
# encouraged to load and test the software's suitability as regards their
# requirements in conditions enabling the security of their systems and/or
# data to be ensured and, more generally, to use and operate it in the
# same conditions as regards security.
#
# The fact that you are presently reading this means that you have had
# knowledge of the CeCILL license and that you accept its terms.

__all__ = ['cantab', 'imaging']


from . import cantab
__all__.extend(cantab.__all__)
from .cantab import check_cant_name
from .cantab import check_datasheet_name
from .cantab import check_detailed_datasheet_name
from .cantab import check_report_name
from .cantab import check_cant_content
from .cantab import check_datasheet_content
from .cantab import check_detailed_datasheet_content
from .cantab import check_report_content

from . import imaging
__all__.extend(imaging.__all__)
from .imaging import check_zip_name
from .imaging import check_zip_content
from .imaging import ZipTree
