import logging
import pyodbc
from sevicios_app.vo.semanaComprasMx import SemanaComprasMX

logger = logging.getLogger('')

class ShopChina():

    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.31'
            nombre_bd = 'COMPRAS_CHINA'
            nombre_usuario = 'soportedb'
            password = 'T+Sab!G@(N)'
            conexion = None

            conexion = pyodbc.connect('DRIVER={SQL Server};SERVER=' + direccion_servidor+';DATABASE='+nombre_bd+';UID='+nombre_usuario+';PWD=' + password)
            return conexion
        except Exception as exception:
            logger.error(f"Se presento un error al establecer la conexion: {exception}")
            raise exception

    def closeConexion(self, conexion):
        try:
            conexion.close()
        except Exception as exception:
            logger.error(f"Se presento una incidencia al cerrar la conexion: {exception}")
            raise exception
    
    def weekShop(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            cursor.execute("""
                            select  ISNULL(tres.sku,isnull(cuatro.sku,cinco.sku)) SKU,

                            ISNULL(tres.[sem1] ,'0') as  sem1_2023,ISNULL(tres.[sem2] ,'0') as  sem2_2023,ISNULL(tres.[sem3] ,'0') as  sem3_2023,
                            ISNULL(tres.[sem4] ,'0') as  sem4_2023,ISNULL(tres.[sem5] ,'0') as  sem5_2023,ISNULL(tres.[sem6] ,'0') as  sem6_2023,
                            ISNULL(tres.[sem7] ,'0') as  sem7_2023,ISNULL(tres.[sem8] ,'0') as  sem8_2023,ISNULL(tres.[sem9] ,'0') as  sem9_2023,
                            ISNULL(tres.[sem10],'0')  as sem10_2023,ISNULL(tres.[sem11],'0')  as sem11_2023,ISNULL(tres.[sem12],'0')  as sem12_2023,
                            ISNULL(tres.[sem13],'0')  as sem13_2023,ISNULL(tres.[sem14],'0')  as sem14_2023,ISNULL(tres.[sem15],'0')  as sem15_2023,
                            ISNULL(tres.[sem16],'0')  as sem16_2023,ISNULL(tres.[sem17],'0')  as sem17_2023,ISNULL(tres.[sem18],'0')  as sem18_2023,
                            ISNULL(tres.[sem19],'0')  as sem19_2023,ISNULL(tres.[sem20],'0')  as sem20_2023,ISNULL(tres.[sem21],'0')  as sem21_2023,
                            ISNULL(tres.[sem22],'0')  as sem22_2023,ISNULL(tres.[sem23],'0')  as sem23_2023,ISNULL(tres.[sem24],'0')  as sem24_2023,
                            ISNULL(tres.[sem25],'0')  as sem25_2023,ISNULL(tres.[sem26],'0')  as sem26_2023,ISNULL(tres.[sem27],'0')  as sem27_2023,
                            ISNULL(tres.[sem28],'0')  as sem28_2023,ISNULL(tres.[sem29],'0')  as sem29_2023,ISNULL(tres.[sem30],'0')  as sem30_2023,
                            ISNULL(tres.[sem31],'0')  as sem31_2023,ISNULL(tres.[sem32],'0')  as sem32_2023,ISNULL(tres.[sem33],'0')  as sem33_2023,
                            ISNULL(tres.[sem34],'0')  as sem34_2023,ISNULL(tres.[sem35],'0')  as sem35_2023,ISNULL(tres.[sem36],'0')  as sem36_2023,
                            ISNULL(tres.[sem37],'0')  as sem37_2023,ISNULL(tres.[sem38],'0')  as sem38_2023,ISNULL(tres.[sem39],'0')  as sem39_2023,
                            ISNULL(tres.[sem40],'0')  as sem40_2023,ISNULL(tres.[sem41],'0')  as sem41_2023,ISNULL(tres.[sem42],'0')  as sem42_2023,
                            ISNULL(tres.[sem43],'0')  as sem43_2023,ISNULL(tres.[sem44],'0')  as sem44_2023,ISNULL(tres.[sem45],'0')  as sem45_2023,
                            ISNULL(tres.[sem46],'0')  as sem46_2023,ISNULL(tres.[sem47],'0')  as sem47_2023,ISNULL(tres.[sem48],'0')  as sem48_2023,
                            ISNULL(tres.[sem49],'0')  as sem49_2023,ISNULL(tres.[sem50],'0')  as sem50_2023,ISNULL(tres.[sem51],'0')  as sem51_2023,
                            ISNULL(tres.[sem52],'0')  as sem52_2023,ISNULL(tres.[sem53],'0')  as sem53_2025,ISNULL(tres.[sem1] ,'0') as  sem1_2024,
                            ISNULL(tres.[sem2] ,'0') as  sem2_2024,ISNULL(tres.[sem3] ,'0') as  sem3_2024,ISNULL(tres.[sem4] ,'0') as  sem4_2024,
                            ISNULL(tres.[sem5] ,'0') as  sem5_2024,ISNULL(tres.[sem6] ,'0') as  sem6_2024,ISNULL(tres.[sem7] ,'0') as  sem7_2024,
                            ISNULL(tres.[sem8] ,'0') as  sem8_2024,ISNULL(tres.[sem9] ,'0') as  sem9_2024,ISNULL(tres.[sem10],'0')  as sem10_2024,
                            ISNULL(tres.[sem11],'0')  as sem11_2024,ISNULL(tres.[sem12],'0')  as sem12_2024,ISNULL(tres.[sem13],'0')  as sem13_2024,
                            ISNULL(tres.[sem14],'0')  as sem14_2024,ISNULL(tres.[sem15],'0')  as sem15_2024,ISNULL(tres.[sem16],'0')  as sem16_2024,
                            ISNULL(tres.[sem17],'0')  as sem17_2024,ISNULL(tres.[sem18],'0')  as sem18_2024,ISNULL(tres.[sem19],'0')  as sem19_2024,
                            ISNULL(tres.[sem20],'0')  as sem20_2024,ISNULL(tres.[sem21],'0')  as sem21_2024,ISNULL(tres.[sem22],'0')  as sem22_2024,
                            ISNULL(tres.[sem23],'0')  as sem23_2024,ISNULL(tres.[sem24],'0')  as sem24_2024,ISNULL(tres.[sem25],'0')  as sem25_2024,
                            ISNULL(tres.[sem26],'0')  as sem26_2024,ISNULL(tres.[sem27],'0')  as sem27_2024,ISNULL(tres.[sem28],'0')  as sem28_2024,
                            ISNULL(tres.[sem29],'0')  as sem29_2024,ISNULL(tres.[sem30],'0')  as sem30_2024,ISNULL(tres.[sem31],'0')  as sem31_2024,
                            ISNULL(tres.[sem32],'0')  as sem32_2024,ISNULL(tres.[sem33],'0')  as sem33_2024,ISNULL(tres.[sem34],'0')  as sem34_2024,
                            ISNULL(tres.[sem35],'0')  as sem35_2024,ISNULL(tres.[sem36],'0')  as sem36_2024,ISNULL(tres.[sem37],'0')  as sem37_2024,
                            ISNULL(tres.[sem38],'0')  as sem38_2024,ISNULL(tres.[sem39],'0')  as sem39_2024,ISNULL(tres.[sem40],'0')  as sem40_2024,
                            ISNULL(tres.[sem41],'0')  as sem41_2024,ISNULL(tres.[sem42],'0')  as sem42_2024,ISNULL(tres.[sem43],'0')  as sem43_2024,
                            ISNULL(tres.[sem44],'0')  as sem44_2024,ISNULL(tres.[sem45],'0')  as sem45_2024,ISNULL(tres.[sem46],'0')  as sem46_2024,
                            ISNULL(tres.[sem47],'0')  as sem47_2024,ISNULL(tres.[sem48],'0')  as sem48_2024,ISNULL(tres.[sem49],'0')  as sem49_2024,
                            ISNULL(tres.[sem50],'0')  as sem50_2024,ISNULL(tres.[sem51],'0')  as sem51_2024,ISNULL(tres.[sem52],'0')  as sem52_2024,
                            ISNULL(tres.[sem53],'0')  as sem53_2024,ISNULL(tres.[sem1] ,'0') as  sem1_2025,ISNULL(tres.[sem2] ,'0') as  sem2_2025,
                            ISNULL(tres.[sem3] ,'0') as  sem3_2025,ISNULL(tres.[sem4] ,'0') as  sem4_2025,ISNULL(tres.[sem5] ,'0') as  sem5_2025,
                            ISNULL(tres.[sem6] ,'0') as  sem6_2025,ISNULL(tres.[sem7] ,'0') as  sem7_2025,ISNULL(tres.[sem8] ,'0') as  sem8_2025,
                            ISNULL(tres.[sem9] ,'0') as  sem9_2025,ISNULL(tres.[sem10],'0')  as sem10_2025,ISNULL(tres.[sem11],'0')  as sem11_2025,
                            ISNULL(tres.[sem12],'0')  as sem12_2025,ISNULL(tres.[sem13],'0')  as sem13_2025,ISNULL(tres.[sem14],'0')  as sem14_2025,
                            ISNULL(tres.[sem15],'0')  as sem15_2025,ISNULL(tres.[sem16],'0')  as sem16_2025,ISNULL(tres.[sem17],'0')  as sem17_2025,
                            ISNULL(tres.[sem18],'0')  as sem18_2025,ISNULL(tres.[sem19],'0')  as sem19_2025,ISNULL(tres.[sem20],'0')  as sem20_2025,
                            ISNULL(tres.[sem21],'0')  as sem21_2025,ISNULL(tres.[sem22],'0')  as sem22_2025,ISNULL(tres.[sem23],'0')  as sem23_2025,
                            ISNULL(tres.[sem24],'0')  as sem24_2025,ISNULL(tres.[sem25],'0')  as sem25_2025,ISNULL(tres.[sem26],'0')  as sem26_2025,
                            ISNULL(tres.[sem27],'0')  as sem27_2025,ISNULL(tres.[sem28],'0')  as sem28_2025,ISNULL(tres.[sem29],'0')  as sem29_2025,
                            ISNULL(tres.[sem30],'0')  as sem30_2025,ISNULL(tres.[sem31],'0')  as sem31_2025,ISNULL(tres.[sem32],'0')  as sem32_2025,
                            ISNULL(tres.[sem33],'0')  as sem33_2025,ISNULL(tres.[sem34],'0')  as sem34_2025,ISNULL(tres.[sem35],'0')  as sem35_2025,
                            ISNULL(tres.[sem36],'0')  as sem36_2025,ISNULL(tres.[sem37],'0')  as sem37_2025,ISNULL(tres.[sem38],'0')  as sem38_2025,
                            ISNULL(tres.[sem39],'0')  as sem39_2025,ISNULL(tres.[sem40],'0')  as sem40_2025,ISNULL(tres.[sem41],'0')  as sem41_2025,
                            ISNULL(tres.[sem42],'0')  as sem42_2025,ISNULL(tres.[sem43],'0')  as sem43_2025,ISNULL(tres.[sem44],'0')  as sem44_2025,
                            ISNULL(tres.[sem45],'0')  as sem45_2025,ISNULL(tres.[sem46],'0')  as sem46_2025,ISNULL(tres.[sem47],'0')  as sem47_2025,
                            ISNULL(tres.[sem48],'0')  as sem48_2025,ISNULL(tres.[sem49],'0')  as sem49_2025,ISNULL(tres.[sem50],'0')  as sem50_2025,
                            ISNULL(tres.[sem51],'0')  as sem51_2025,ISNULL(tres.[sem52],'0')  as sem52_2025,ISNULL(tres.[sem53],'0')  as sem53_2025
                            from
                            (
                            SELECT [sku],[sem1],[sem2],[sem3],[sem4],[sem5],[sem6],[sem7],[sem8],[sem9],[sem10],[sem11],[sem12],[sem13],
                                [sem14],[sem15],[sem16],[sem17],[sem18],[sem19],[sem20],[sem21],[sem22],[sem23],[sem24],[sem25],[sem26],
                                [sem27],[sem28],[sem29],[sem30],[sem31],[sem32],[sem33],[sem34],[sem35],[sem36],[sem37],[sem38],[sem39],
                                [sem40],[sem41],[sem42],[sem43],[sem44],[sem45],[sem46],[sem47],[sem48],[sem49],[sem50],[sem51],[sem52],[sem53]
                            FROM [COMPRAS_CHINA].[dbo].[BUFFERS]
                            where anio='2023'
                            and pais='MX'
                            )tres 
                            full join 
                            (SELECT [sku],[sem1],[sem2],[sem3],[sem4],[sem5],[sem6],[sem7],[sem8],[sem9],[sem10],[sem11],[sem12],[sem13],
                                [sem14],[sem15],[sem16],[sem17],[sem18],[sem19],[sem20],[sem21],[sem22],[sem23],[sem24],[sem25],[sem26],
                                [sem27],[sem28],[sem29],[sem30],[sem31],[sem32],[sem33],[sem34],[sem35],[sem36],[sem37],[sem38],[sem39],
                                [sem40],[sem41],[sem42],[sem43],[sem44],[sem45],[sem46],[sem47],[sem48],[sem49],[sem50],[sem51],[sem52],[sem53]
                            FROM [COMPRAS_CHINA].[dbo].[BUFFERS]
                            where anio='2024'
                            and pais='MX')cuatro on cuatro.sku=tres.sku
                            full join (SELECT [sku],[sem1],[sem2],[sem3],[sem4],[sem5],[sem6],[sem7],[sem8],[sem9],[sem10],[sem11],[sem12],[sem13],
                                [sem14],[sem15],[sem16],[sem17],[sem18],[sem19],[sem20],[sem21],[sem22],[sem23],[sem24],[sem25],[sem26],
                                [sem27],[sem28],[sem29],[sem30],[sem31],[sem32],[sem33],[sem34],[sem35],[sem36],[sem37],[sem38],[sem39],
                                [sem40],[sem41],[sem42],[sem43],[sem44],[sem45],[sem46],[sem47],[sem48],[sem49],[sem50],[sem51],[sem52],[sem53]
                            FROM [COMPRAS_CHINA].[dbo].[BUFFERS]
                            where anio='2025'
                            and pais='MX')cinco on cinco.sku=cuatro.sku
                            order by tres.sku
                            """)
            registros=cursor.fetchall()
            for registro in registros:
                pedido=SemanaComprasMX(registro[0],registro[1],registro[2],registro[3],registro[4],registro[5],registro[6],registro[7],registro[8],registro[9],registro[10],registro[11],registro[12],registro[13],registro[14],registro[15],registro[16],registro[17],registro[18],registro[19],registro[20],registro[21],registro[22],registro[23],registro[24],registro[25],registro[26],registro[27],registro[28],registro[29],registro[30],registro[31],registro[32],registro[33],registro[34],registro[35],registro[36],registro[37],registro[38],registro[39],registro[40],registro[41],registro[42],registro[43],registro[44],registro[45],registro[46],registro[47],registro[48],registro[49],registro[50],registro[51],registro[52],registro[53],registro[54],registro[55],registro[56],registro[57],registro[58],registro[59],registro[60],registro[61],registro[62],registro[63],registro[64],registro[65],registro[66],registro[67],registro[68],registro[69],registro[70],registro[71],registro[72],registro[73],registro[74],registro[75],registro[76],registro[77],registro[78],registro[79],registro[80],registro[81],registro[82],registro[83],registro[84],registro[85],registro[86],registro[87],registro[88],registro[89],registro[90],registro[91],registro[92],registro[93],registro[94],registro[95],registro[96],registro[97],registro[98],registro[99],registro[100],registro[101],registro[102],registro[103],registro[104],registro[105],registro[106],registro[107],registro[108],registro[109],registro[110],registro[111],registro[112],registro[113],registro[114],registro[115],registro[116],registro[117],registro[118],registro[119],registro[120],registro[121],registro[122],registro[123],registro[124],registro[125],registro[126],registro[127],registro[128],registro[129],registro[130],registro[131],registro[132],registro[133],registro[134],registro[135],registro[136],registro[137],registro[138],registro[139],registro[140],registro[141],registro[142],registro[143],registro[144],registro[145],registro[146],registro[147],registro[148],registro[149],registro[150],registro[151],registro[152],registro[153],registro[154],registro[155],registro[156],registro[157],registro[158],registro[159])
                pedidosList.append(pedido)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            print(exception)
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)