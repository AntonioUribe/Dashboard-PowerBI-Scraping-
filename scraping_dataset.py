# Fuente_ https://pawarbi.github.io/blog/ppu/xmla/powerbi_premium/premium/python/jupyter_notebook/2020/12/11/Accessing-Power-BI-Datasets-via-XMLA-Endpoint-in-Python-Jupyter-Notebook.html
import pandas as pd
import ssas_api as ssas



class SSASConnector:
    def __init__(self, server, username, password):
        self.server = server
        self.username = username
        self.password = password
    
    def connect_to_workspace(self):
        
        conn_string = ssas.set_conn_string(
            server=self.server,
            db_name='',
            username=f'{self.username}',
            password=f'{self.password}'
        )
        
        global System, DataTable, AMO, ADOMD
        import System
        from System.Data import DataTable
        import Microsoft.AnalysisServices.Tabular as TOM
        import Microsoft.AnalysisServices.AdomdClient as ADOMD
        try:
            self.TOMServer = TOM.Server()
            self.TOMServer.Connect(conn_string)
            print("Connection to Workspace Successful !")
        except Exception as e:
            print("Connection to Workspace Failed")
            raise e
    
    def get_datasets_info(self):
        #Crear un DataFrame con la información de los Datasets
        datasets = pd.DataFrame(columns=['Dataset_Name', 'Compatibility', 'ID', 'Size_MB', 'Created_Date', 'Last_Update'])
        for item in self.TOMServer.Databases:
            datasets.loc[len(datasets)] = {
                'Dataset_Name': item.Name,
                'Compatibility': item.CompatibilityLevel,
                'Created_Date': item.CreatedTimestamp,
                'ID': item.ID,
                'Last_Update': item.LastUpdate,
                'Size_MB': (item.EstimatedSize * 1e-06)
            }
        display(datasets)
        while True:
            try:
                dataset_info = input("Insert ID: ")
                break
            except:
                print("Error ID incorrecto")
                input("Vuelve a intentarlo: \nPress to <Enter>")
        #Select Name Dataset
        #Agregar el ID correspondiente del Dataset de Power BI

        dataset_name = datasets[datasets['ID'].isin([dataset_info])].Dataset_Name.tolist()[0]
        return dataset_name,dataset_info
    
    def get_table_names(self, database_name):
        ds = self.TOMServer.Databases[database_name]
        table_names = [table.Name for table in ds.Model.Tables]
        return table_names
    
    def execute_dax_query(self, database_name, dax_query):
        conn_string = ssas.set_conn_string(
            server=self.server,
            db_name=database_name,
            username=self.username,
            password=self.password
        )
        df_result = ssas.get_DAX(connection_string=conn_string, dax_string=dax_query)
        return df_result
    def limpiar_df(self,df):
        df =result_df.copy()
        df.columns.tolist()

        columnas_nuevas = []
        for cadena in df.columns.tolist():
            
            palabras = cadena.split('[')
            palabras = palabras[1].split(']')
            palabras = palabras[0].replace(" ", "")
            print(palabras)
            columnas_nuevas.append(palabras)
        df.columns = columnas_nuevas
        return df

# Example usage:
#settings > PowerBI settings > Server Settings >Connection string > [server]
server = 'powerbi://api.powerbi.com/v1.0/myorg/[workspace];Initial Catalog=[name_catalog];'
#Add email and password
username = ' '
password = ' '

if __name__ == "__main__":
    ssas_connector = SSASConnector(server, username, password)


    ssas_connector.connect_to_workspace()
    dataset_name,dataset_info = ssas_connector.get_datasets_info()
    table_names = ssas_connector.get_table_names(dataset_info)
    #Agregar la consulta DAX que se desea realizar
    dax_query = f'''
        //Ejemplo de consulta DAX para Agregar una tabla
        EVALUATE
            TOPN(10,'TABLE EXAMPLE')
        '''
    result_df = ssas_connector.execute_dax_query(dataset_name, dax_query)
    df = ssas_connector.limpiar_df(result_df)