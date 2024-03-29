=========================================
Databank operations of the Imagen project
=========================================

Databank operations are mostly documented internally at NeuroSpin.

Basic information is available from the `project wiki`_.

This Python package combines a Python library *imagen_databank* for basic
sanity check and preprocessing of Imagen data and a set of scripts to
extract, check, anonymize and transform raw Imagen data.

``imagen_databank``
  Read and perform sanity checks on raw datasets.

``cantab``
  Extract age from FU2 Cantab data.

``dawba``
  Remove identifying data and convert PSC1 to PSC2 in Dawba data,
  after manual download from the youthinmind_ server.

``stratify_demographics``
  Cross-check Stratify age and sex with `stratify_debug_psytools.py`.
  Print demographics with `demographics.py`, using recruitment files and
  validated age/sex from the output of teh previosu script.

``geolocation``
  Merge and convert geolocation data from PSC1 to PSC2.

``mri``
  De-identify some NIfTI files that used to contain the PSC1 code.

``onsets``
  Remove identifying data and convert PSC1 to PSC2 in FU3 onsets files.

``psc``
  Update FU3 Dawba codes from token tables maintained on the Delosis_ serevr.

``psytools``
  Download Psytools data as CSV files from the Delosis_ server.
  Remove identifying data and convert PSC1 to PSC2.

``sex``
  Derive reference sex of Imagen subjects from multiple sources.
  There had been errors at baseline.

.. _`project wiki`: https://github.com/imagen2/imagen_databank/wiki
.. _youthinmind: http://youthinmind.com
.. _Delosis: https://www.delosis.com
