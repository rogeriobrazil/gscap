#!/usr/bin/env python
# -*- coding: utf-8 -*-


import unittest
import cvm


class Test_cvm(unittest.TestCase):

    def test_process_file(self):
        do_proc = cvm.processa_arquivo("02", 2019)
        self.assertEqual(do_proc, True)

    def test_consulta_cnpj(self):
        do_consulta = cvm.consulta_dados("28580807000160")
        self.assertEqual(do_consulta, True)

        # CNPJs 
        # 28581055000151
        # 28580807000160
