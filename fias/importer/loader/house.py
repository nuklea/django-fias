#coding: utf-8
from __future__ import unicode_literals, absolute_import

from fias.importer.bulk import BulkCreate
from fias.models import AddrObj, House
from .base import LoaderBase


class Loader(LoaderBase):

    def _init(self):
        self._model = House
        self._bulk = BulkCreate(House, 'houseguid', 'updatedate')
        self.aoguids = list(AddrObj.objects.values_list('pk', flat=True))

    def process_row(self, row):
        if row.tag == 'House':
            end_date = self._str_to_date(row.attrib['ENDDATE'])
            if end_date < self._today:
                print ('Out of date entry. Skipping...')
                return
    
            start_date = self._str_to_date(row.attrib['STARTDATE'])
            if start_date > self._today:
                print ('Date in future - skipping...')
                return
    
            related_attrs = dict()

            if row.attrib['AOGUID'] not in self.aoguids:
                print ('AddrObj with GUID `{0}` not found. Skipping house...'.format(row.attrib['AOGUID']))
                return

            related_attrs['aoguid'] = AddrObj(pk=row.attrib['AOGUID'])
            self._bulk.push(row, related_attrs=related_attrs)
