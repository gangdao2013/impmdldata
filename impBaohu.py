#-*- coding: utf-8 -*-  
import codecs, sys, ctypes, platform, os
import re
from collections import OrderedDict

# 导入保护信号
class BaoHu:
        context = 'realtime'
        app = 'public'

        def __init__(self):
                self._id_rdf = {} # 源id-rdf
                self._stanames = {} # 厂站id-名
                self._feednames = {} # 馈线id-名
                self._devInfo = {} # 设备id-名
                self._tarRdf_id = {} # 目标库的rdf-id
                self._baohus = [] # 新创建保护设备
                self._protEquips = [] # 新创建保护设备

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
                bjlxAll = (5,6,8,9,11,24) # 厂站 母线 开关 刀闸 变压器 馈线
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
                                        self._id_rdf[self.makeId(bjlx, 0, bjid)] = flds[rdfNo]
                                
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
                fldMap = {2:'MINGZI', 3:'BIANHAO', 4:'ID', 12:'DELAYTIME', 15:'GZFLAG', 16:'ALARMLEVEL'} # 800 - 1000e
                pstMap = {1:1, 2:13, 3:14, 253:15, 254:16} # 1000e - 800 ProtectionSignalType
                tarFldNos = range(1, 26)
                line = ''
                with codecs.open('baohu.csv', 'r') as f:
                        line = f.readline().replace('\n', '')
                        f.close()
                fldNames = line.split(',')
                bjlxNo = fldNames.index('BUJIANLEIXINGID')
                bjidNo = fldNames.index('BUJIANID')
                feedNo = fldNames.index('FEEDERID')
                czidNo = fldNames.index('CHANGZHANID')
                bhlxNo = fldNames.index('BAOHULEIXINGID')
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
                                rid = self.makeId(bjlx, 0, bjid)
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
                                        feedRid = self.makeId(24, 0, feedid) # 24 1000e馈线表
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
                                        czRid = self.makeId(5, 0, czid) # 5 1000e厂站表
                                        if czRid in self._id_rdf:
                                                czrdf = self._id_rdf[czRid]
                                                if czrdf in self._tarRdf_id:
                                                        staResId = int(self._tarRdf_id[czrdf])
                                        cz_resId[czid] = staResId
                                else:
                                        staResId = cz_resId[czid]

                        if devResId > 0 or staResId > 0:
                                houseResId = 0    
                                tabNo = staResId >> 48 & 0xffff
                                if tabNo == 13: # Substation_B
                                        pass
                                elif tabNo == 153: # DistributeStation_B
                                        houseResId = staResId
                                        staResId = 0

                                name = pathname = ''
                                if devResId in self._devInfo:
                                        feedResId = self._devInfo[devResId][0]
                                if feedResId in self._feednames:
                                        (staResId, sta, feed) = self._feednames[feedResId]
                                        (name, pathname) = self.delDup(sta, feed, flds[fldMap[2]])
                                        #pathname = '%s.%s.%s' % (''.join(sta), feed, flds[fldMap[2]])
                                elif staResId in self._stanames:
                                        (name, pathname) = self.delDup(sta, '', flds[fldMap[2]])
                                        #pathname = '%s.%s' % (''.join(self._stanames[staResId]), flds[fldMap[2]])
                                else:
                                        pathname = '%s' % (flds[fldMap[2]])

                                tmpKey = 0
                                if devResId > 0:
                                        tmpKey = devResId
                                elif staResId > 0:
                                        tmpKey = staResId
                                else:
                                        tmpKey = houseResId
                                
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
                                                record = '%s'%(self.makeId(38, 120, index)) # 38 保护信号表  120 ProtectionEquipmentStatus
                                        elif i == 5: #pathname
                                                record += ',%s' % (pathname)
                                        elif i == 7: # Substation_ID
                                                record += ',%s'%(staResId)
                                        elif i == 10: # ProtectionEquipment_ID
                                                record += ',%s'%(protEquipId)
                                        elif i == 13: # ProtectionSignalType
                                                tarSigType = 0
                                                sigType = int(flds[bhlxNo])
                                                if sigType in pstMap:
                                                        tarSigType = pstMap[sigType]
                                                record += ',%s'%(tarSigType)
                                        elif i in (14, 17, 18, 19): # 14-ShowConditionEvent 17-ProtectionCatagory 18-EnableConditionEvent 19-EnableControl
                                                record += ',1'
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
                protEquipResId = self.makeId(34, 0, index)  # ProtectionEquipment_B
                record = u''
                for i in range(1, 16):
                        if i == 1:
                                record = '%s'%(protEquipResId)
                        elif i == 2: # name
                                record += ',保护'
                        elif i == 5: # pathname
                                tmp = []
                                if devResId in self._devInfo:
                                        feedResId = self._devInfo[devResId][0]
                                        tmp.append(self._devInfo[devResId][1])
                                if feedResId in self._feednames:
                                        (unused, sta, feed) = self._feednames[feedResId]
                                        tmp.insert(0, feed)
                                        tmp.insert(0, ''.join(sta))
                                elif staResId in self._stanames:
                                        tmp.insert(0, ''.join(self._stanames[staResId]))
                                tmp.append('保护')
                                record += ',%s' % ('.'.join(tmp))
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

        def makeId(self, bjlx, bjcs, bjid):
                return (bjlx<<48 & 0xFFFF000000000000) | (bjcs << 32 & 0xFFFF00000000) | (bjid & 0xFFFFFF)

        # 读取目标库的rdf
        def getTarRdf(self):
                print('读取目标库rdf')
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

        # 厂站名
        def getStaNames(self):
                print('读取目标库厂站名')
                count = 2
                with codecs.open('Substation_B.txt', 'r') as f:
                        line = f.readline().replace(' ', '')
                        fldNames = line.split('\t')
                        idNo = fldNames.index('厂站ID')
                        nameNo = fldNames.index('名称')
                        line = f.readline().replace('\n', '')
                        while line:
                                flds = line.split('\t')
                                if len(flds) != len(fldNames):
                                        print('Bad format: %s != %s, line:%s' % (len(flds), len(fldNames), count))
                                id = int(flds[idNo])
                                name = flds[nameNo]
                                pos = name.find('kV')
                                if pos >= 0:
                                        self._stanames[id] = (name[:pos+2], name[pos+2:])
                                else:
                                        self._stanames[id] = ('', name)
                                # print('%s:%s-%s' % (name, self._stanames[id][0], self._stanames[id][1]))
                                line = f.readline().replace('\n', '')
                                count += 1
                        f.close()

        # 馈线名
        def getFeedNames(self):
                print('读取目标库馈线名')
                count = 2
                with codecs.open('Feeder_B.txt', 'r') as f:
                        line = f.readline().replace(' ', '')
                        fldNames = line.split('\t')
                        idNo = fldNames.index('ID')
                        nameNo = fldNames.index('名称')
                        staNo = fldNames.index('所属变电站')
                        line = f.readline().replace('\n', '')
                        while line:
                                flds = line.split('\t')
                                if len(flds) != len(fldNames):
                                        print('Bad format: %s != %s, line:%s' % (len(flds), len(fldNames), count))
                                id = int(flds[idNo])
                                staId = int(flds[staNo])
                                feedname = flds[nameNo]
                                if staId in self._stanames:
                                        for i in self._stanames[staId]:
                                                feedname = feedname.replace(i, '', 1)
                                                self._feednames[id] = (staId, self._stanames[staId], feedname)
                                        # print('%s-%s:%s' % (self._stanames[staId], flds[nameNo], feedname))
                                else:
                                        self._feednames[id] = (0, ('', ''), feedname)
                                line = f.readline().replace('\n', '')
                                count += 1
                        f.close()

        # 获取目标库设备表数据
        def getDev(self, tab):
                print('读取目标库%s名' % (tab))
                count = 2
                with codecs.open('%s.txt' % (tab), 'r') as f:
                        line = f.readline().replace(' ', '')
                        fldNames = line.split('\t')
                        idNo = 0 # ID
                        nameNo = 2 # 名称
                        feedNo = fldNames.index('所属馈线')
                        line = f.readline().replace('\n', '')
                        while line:
                                flds = line.split('\t')
                                if len(flds) != len(fldNames):
                                        print('Bad format: %s != %s, line:%s' % (len(flds), len(fldNames), count))
                                id = int(flds[idNo])
                                feedId = int(flds[feedNo])
                                devname = flds[nameNo]
                                self._devInfo[id] = (feedId, devname)
                                line = f.readline().replace('\n', '')
                                count += 1
                        f.close()

        # 数据字段和字段模式个数不匹配的纠正
        def modiFlds(self, fldNames, flds, badFldNo):
                badLast = len(flds)- (len(fldNames) - badFldNo - 1)
                content = flds[badFldNo : badLast]
                return flds[:badFldNo] + [u'、'.join(content)] + flds[badLast:]

        # 去重
        def delDup(self, sta, feed, name):
                pathname = ''
                pattern = re.compile(r'\d+')
                sections = name.split('/')
                if len(sections) > 2:
                        tmp1 = sections[0]
                        if u'福州.' in tmp1:
                                tmp1 = tmp1.replace(u'福州.', '', 1)
                        if tmp1 in sta:
                                sections.pop(0)
                                tmp1 = sections[0]
                                if 'kV' in tmp1 and len(feed) > 0:
                                        tmp1 = tmp1[tmp1.index('kV')+2:]
                                        tmp1 = tmp1.replace('.', '')
                                        r = pattern.findall(tmp1)
                                        for i in r:
                                                if i in feed:
                                                        tmp1 = tmp1.replace(i, '', 1)
                                        for ii in (u'线回路'):
                                                tmp1 = tmp1.replace(ii, '')
                                        if tmp1 in feed:
                                                sections.pop(0)
                modiname = '/'.join(sections)
                if len(feed) > 0:
                        pathname = '%s.%s.%s' % (''.join(sta), feed, modiname)
                else:
                        pathname = '%s.%s' % (''.join(sta), modiname)
                return modiname, pathname

        
if __name__ == '__main__':
        b = BaoHu()
        b.prepare()
        b.getStaNames()
        b.getFeedNames()
        for tab in ('Breaker_B', 'Disconnector_B', 'BusbarSection_B', 'CompositeSwitch_B'):
                b.getDev(tab)
        b.getTarRdf()
        b.getRdf()
        b.crtProtSig()
        b.writeProtEquip()
