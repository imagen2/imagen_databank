# noqa

# Copyright (c) 2014-2018 CEA
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

__all__ = ['additional_data', 'behavioral', 'cantab', 'core', 'dicom_utils',
           'image_data', 'scanning', 'sanity']

from . import core
from .core import (LONDON, NOTTINGHAM, DUBLIN, BERLIN,
                   HAMBURG, MANNHEIM, PARIS, DRESDEN,
                   SOUTHAMPTON, AACHEN)
from .core import CENTER_NAME
from .core import (PSC2_FROM_PSC1, PSC1_FROM_PSC2,
                   PSC1_FROM_DAWBA, PSC2_FROM_DAWBA,  # PSC2_FROM_DAWBA is obsolete
                   DOB_FROM_PSC1, DOB_FROM_PSC2)  # DOB_FROM_PSC2 is obsolete
from .core import (detect_psc1, detect_psc2, guess_psc1)
from .core import Error

from . import additional_data
from .additional_data import (walk_additional_data, report_additional_data)

from . import behavioral
from .behavioral import (MID_CSV, FT_CSV, SS_CSV, RECOG_CSV)
from .behavioral import (read_mid, read_ft, read_ss, read_recog)

from . import cantab
from .cantab import (CANTAB_CCLAR, DETAILED_DATASHEET_CSV, DATASHEET_CSV,
                     REPORT_HTML)
from .cantab import (read_cant, read_datasheet, read_detailed_datasheet,
                     read_report)

from . import dicom_utils
from .dicom_utils import read_metadata

from . import image_data
from .image_data import (SEQUENCE_LOCALIZER_CALIBRATION,
                         SEQUENCE_T2, SEQUENCE_T2_FLAIR,
                         SEQUENCE_ADNI_MPRAGE,
                         SEQUENCE_MID, SEQUENCE_FT, SEQUENCE_SST,
                         SEQUENCE_B0_MAP, SEQUENCE_DTI,
                         SEQUENCE_RESTING_STATE,
                         SEQUENCE_NODDI)
from .image_data import SEQUENCE_NAME
from .image_data import NONSTANDARD_DICOM
from .image_data import series_type_from_description
from .image_data import walk_image_data, report_image_data

from . import scanning
from .scanning import read_scanning

from . import sanity

__author__ = 'Dimitri Papadopoulos'
__copyright__ = 'Copyright (c) 2014-2018 CEA'
__license__ = 'CeCILL'
__version__ = '0.1.0'
__email__ = 'imagendatabase@cea.fr'
__status__ = 'Development'
