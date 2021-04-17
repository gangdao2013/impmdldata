#-*- coding: utf-8 -*-  
import codecs, sys, ctypes, platform, os
from collections import OrderedDict

# 导入保护信号
class BaoHu:
	
        context = 'realtime'
        app = 'public'
        
        _id_rdf = {}
        _tarRdf_id = {} # 目标库的rdf-id
        _baohus = [] # 新创建保护设备
        _protEquips = [] # 新创建保护设备

        def __init__(self):
                for i in os.environ.get('path').split(';'):
                        if len(i) > 0 and os.path.isdir(i):
                                os.add_dll_directory(i)
                                
                sysstr = platform.system()
                if(sysstr == "Windows"):
                        self.lib = ctypes.cdll.LoadLibrary('impmodel_d')
                elif(sysstr == "Linux"):
                        self.lib = ctypes.cdll.LoadLibrary('libimpmodel.so')
        
        # 预处理保护信号，统计保护信号表中一些字段的值的合集
        def prepare(self):
                print('预处理保护信号')
                with codecs.open('baohu.csv', 'r') as f:
                        line = f.readline().replace('\n', '')
                        fldNames = line.split(',')     
                        stats = ['' for i in range(len(fldNames))]                   
                        mzNo = fldNames.index('MINGZI')
                        
                        line = f.readline().replace('\n', '')
                        count=2
                        while line:
                                flds = line.split(',')
                                errFmt = ''
                                if len(flds) != len(fldNames):
                                        errFmt = 'Bad format: %s != %s, line:%s'%(len(flds),len(fldNames), count)
                                        if len(flds) < len(fldNames): # 名字中有换行符导致
                                                count += 1
                                                breakLine = f.readline().replace('\n', '')
                                                if breakLine:
                                                        line += breakLine
                                                        flds = line.split(',')
                                        else: # 名字中有','导致
                                                flds = self.modiFlds(fldNames, flds, mzNo)
                                if len(errFmt)>0 and len(flds) != len(fldNames):
                                                print(errFmt + '--------------err------------------')
                                                
                                if len(errFmt)==0 or len(flds) == len(fldNames):
                                        self._baohus.append(flds)           
                                        for i in range(5, len(fldNames)):
                                                if i != 6 and i != 7:
                                                       if flds[i] in stats[i]:
                                                               pass
                                                       else:
                                                               stats[i] += (flds[i])+','
                                line = f.readline().replace('\n', '')
                                count += 1
                        for i in range(5, len(fldNames)):
                                if i != 6 and i != 7:
                                        print(fldNames[i] + ': ' + stats[i])
                        f.close()

        # 获取部分类型的源rdf
        def getRdf(self):
                print('读取源RDF表')
                bjlxAll = (8,6,11,24,5) # 开关 母线 配变 馈线 厂站
                count = 2
                with codecs.open('rdflist.csv', 'r') as f:
                        line = f.readline().replace('\n', '')
                        fldNames = line.split(',')
                        bjlxNo = fldNames.index('BJTYPE')
                        bjidNo = fldNames.index('BJID')
                        rdfNo = fldNames.index('GISRDFID')
                        gismridNo = fldNames.index('GISMRID')
                                 
                        line = f.readline().replace('\n', '')
                        while line:
                                flds = line.split(',')
                                errFmt = ''
                                if len(flds) != len(fldNames):
                                        errFmt = 'Bad format: %s != %s, line:%s'%(len(flds),len(fldNames), count)
                                        if len(flds) > len(fldNames): # GISMRID中有','导致
                                                flds = self.modiFlds(fldNames, flds, gismridNo)
                                if len(errFmt)>0 and len(flds) != len(fldNames):
                                                print(errFmt + '--------------err------------------')

                                bjlx = int(flds[bjlxNo])
                                if bjlx in bjlxAll:
                                        bjid = int(flds[bjidNo])
                                        self._id_rdf[self.makeId(bjlx, bjid)] = flds[rdfNo]
                                
                                line = f.readline().replace('\n', '')
                                count += 1
                                #if count>100:
                                #        break
                        
                        f.close()
                #print(self._id_rdf)

        # 创建保护信号
        def crtProtSig(self):
                print('写入保护信号')
                tab = 'ProtectionSignal_B'
                fldMap = {2:'MINGZI', 3:'BIANHAO', 4:'ID', 5:'MINGZI', 15:'GZFLAG', 16:'ALARMLEVEL', 12:'DELAYTIME'} # 800 - 1000e
                pstMap = {2:13, 3:14, 253:15, 254:16} # 1000e - 800 ProtectionSignalType
                tarFldNos = range(1, 26)
                line = ''
                with codecs.open('baohu.csv', 'r') as f:
                        line = f.readline().replace('\n', '')
                        f.close()
                fldNames = line.split(',')
                idNo = fldNames.index('ID')
                bjlxNo = fldNames.index('BUJIANLEIXINGID')
                bjidNo = fldNames.index('BUJIANID')
                feedNo = fldNames.index('FEEDERID')
                czidNo = fldNames.index('CHANGZHANID')
                for i in fldMap:
                        fldMap[i] = fldNames.index(fldMap[i])

                dev_resId = feed_resId = cz_resId = {}
                protDev = {} # 设备-保护设备
                count=0
                index = 1
                self.lib.beginWrite(self.context.encode(), self.app.encode(), tab.encode())
                for flds in self._baohus:
                        if len(flds) != len(fldNames):
                                print('Bad format: %s != %s, line:%s'%(len(flds),len(fldNames), count))
                                                                
                        devResId = 0
                        staResId = 0
                        feedResId = 0
                        bjlx = int(flds[bjlxNo])
                        bjid = int(flds[bjidNo])
                        if bjlx > 0 and bjid > 0:                                        
                                rid = self.makeId(bjlx, bjid)
                                if rid not in dev_resId:
                                        if rid in self._id_rdf:
                                                devrdf = self._id_rdf[rid]
                                                if devrdf in self._tarRdf_id:
                                                        devResId = self._tarRdf_id[devrdf]
                                        dev_resId[rid] = devResId
                                else:
                                        devResId = dev_resId[rid]
             
                        feedid = int(flds[feedNo])
                        if feedid > 0:
                                if feedid not in feed_resId:
                                        feedRid = self.makeId(24, feedid) # 24 1000e馈线表
                                        if feedRid in self._id_rdf:
                                                feedrdf = self._id_rdf[feedRid]                                                        
                                                if feedrdf in self._tarRdf_id:
                                                        feedResId = self._tarRdf_id[feedrdf]
                                        feed_resId[feedid] = feedResId
                                else:
                                        feedResId = feed_resId[feedid]
                        
                        czid = int(flds[czidNo])
                        if czid > 0:                                        
                                if czid not in cz_resId:
                                        czRid = self.makeId(5, czid) # 5 1000e厂站表
                                        if czRid in self._id_rdf:
                                                czrdf = self._id_rdf[czRid]
                                                if czrdf in self._tarRdf_id:
                                                        staResId = int(self._tarRdf_id[czrdf])
                                        cz_resId[czid] = staResId
                                else:
                                        staResId = cz_resId[czid]
                                        
                        if devResId > 0 or staResId > 0:                                
                                tmpKey = 0
                                if devResId == 0:
                                        tmpKey = staResId
                                else:
                                        tmpKey = devResId
                                
                                houseResId = 0    
                                tabNo = staResId >> 48 & 0xffff
                                if tabNo == 13: # Substation_B
                                        pass
                                if tabNo == 153: # DistributeStation_B
                                        houseResId = staResId
                                        staResId = 0
                                
                                protEquipId = 0      
                                if tmpKey not in protDev:
                                        protEquipId,data = self.crtProtEquip(len(self._protEquips)+1, devResId, staResId, feedResId, houseResId)
                                        protDev[tmpKey] = protEquipId
                                        self._protEquips.append(data)
                                else:
                                        protEquipId = protDev[tmpKey]

                                record = u''
                                for i in tarFldNos:
                                        if i == 1:
                                                record = '%s'%(self.makeId(38, index)) # 38 保护信号表
                                        elif i == 7: # Substation_ID
                                                record += ',%s'%(staResId)
                                        elif i == 10: # ProtectionEquipment_ID
                                                record += ',%s'%(protEquipId)
                                        elif i == 13: # ProtectionSignalType
                                                tarSigType = 0
                                                sigType = int(flds[bjlxNo])
                                                if sigType in pstMap:
                                                        tarSigType = pstMap[sigType]
                                                record += ',%s'%(tarSigType)
                                        elif i == 21: # Feeder_ID
                                                record += ',%s' % (feedResId)
                                        elif i == 25: # DistributeStation_ID
                                                record += ',%s'%(houseResId)
                                        elif i in fldMap:
                                                record += ',%s' % (flds[fldMap[i]])
                                        else:
                                                record += ','
                                index += 1
                                self.lib.addRecord(record.encode())
                                
                        count += 1
                self.lib.endWrite()        

        # 写入保护设备
        def writeProtEquip(self):
                print('写入保护设备')
                if len(self._protEquips) > 0:
                        tab = 'ProtectionEquipment_B'
                        self.lib.beginWrite(self.context.encode(), self.app.encode(), tab.encode())
                        for record in self._protEquips:
                                self.lib.addRecord(record.encode())
                        self.lib.endWrite()
                                

        # 创建保护设备
        def crtProtEquip(self, index, devResId, staResId, feedResId, houseResId):
                protEquipResId = self.makeId(34, index)  # ProtectionEquipment_B
                record = u''
                for i in range(1, 16):
                        if i == 1:
                                record = '%s'%(protEquipResId)
                        elif i == 2:
                                record += ',保护'
                        elif i == 7: # Substation_ID
                                record += ',%s'%(staResId)
                        elif i == 9: # BaseVoltage_ID
                                record += ',%s'%(2814749767106565) # 10kV
                        elif i == 11: # MasterProtectEquipment_ID
                                record += ',%s'%(devResId)
                        elif i == 13: # Feeder_ID
                                record += ',%s'%(feedResId)
                        elif i == 15: # DistributeStation_ID
                                record += ',%s'%(houseResId)
                        else:
                                record += ','
                return protEquipResId, record

        def makeId(self, bjlx, bjid):
                return bjlx<<48 & 0xFFFF000000000000 | bjid

        # 读取目标库的rdf
        def getTarRdf(self):
                print('读取目标库rdf')
                idNo = 0 # 设备ID
                rdfNo = 1 # GIS资源编码
                count = 2
                with codecs.open('RdfList_B.txt', 'r') as f:
                        line = f.readline().replace(' ', '')
                        fldNames = line.split('\t')
                        idNo = fldNames.index('设备ID')
                        rdfNo = fldNames.index('GIS资源编码')
                                 
                        line = f.readline().replace('\n', '')
                        while line:
                                flds = line.split('\t')
                                if len(flds) != len(fldNames):
                                        print('Bad format: %s != %s, line:%s'%(len(flds),len(fldNames), count))

                                id = int(flds[idNo])
                                rdf = flds[rdfNo]
                                if len(rdf) > 0:
                                        self._tarRdf_id[rdf] = id
                                
                                line = f.readline().replace('\n', '')
                                count += 1
                        
                        f.close()

        # 数据字段和字段模式个数不匹配的纠正
        def modiFlds(self, fldNames, flds, badFldNo):
                badLast = len(flds)- (len(fldNames) - badFldNo - 1)
                content = flds[badFldNo : badLast]
                return flds[:badFldNo] + [','.join(content)] + flds[badLast:]

        
if __name__ == '__main__':
        b = BaoHu()
        b.prepare()
        b.getTarRdf()
        b.getRdf()
        b.crtProtSig()
        b.writeProtEquip()

