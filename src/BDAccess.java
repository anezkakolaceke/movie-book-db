

import java.sql.*;

public class BDAccess implements Database{
	private Connection con;
	public int numero;
	public String resultado;
        //public String[] resultadow=new String[writer_num];
        public String[] resultadow;
        public String[] resultadogb;
        public String[] resultadoe;
        public String[] resultadop;
        public String[] resultadosb;
        public String[] resultadogen;
        public String[][] resultadoBook;
        public String resultadoid;
        public String[][] resultadoFilm;

	private boolean criarConexao(String url, String driver, String login, String senha){
		try{
			Class.forName(driver);
			con=DriverManager.getConnection(url, login, senha);
		}
		catch(java.lang.ClassNotFoundException e){
			System.err.print("Classe do driver de banco de dados não encontrada:"+ e.getMessage());
			return false;
		}
		catch(Exception e){
			System.err.println(e.getMessage());
			return false;
		}
		return true;
	}

	private boolean executaAtualizacao(String sql){
		try{
			Statement stmt=con.createStatement();
			this.numero=stmt.executeUpdate(sql);
			stmt.close();
		}
		catch(SQLException ex){
			System.err.println("Exceção SQL: "+ex.getMessage());
			return false;
		}
		return true;
        }
        
	private void executeQueryWriter(String sql){
			try{
				Statement stmt=con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
                                rs.last();
                                System.out.println(rs.getRow());
                                resultadow=new String[rs.getRow()];
				try{
					int numCols=rs.getMetaData().getColumnCount();
					int x=0;
                                        int count=0;
                                        rs.first();
					do{
       
						this.resultadow[count]=rs.getString("nm_writer");
                                                count++;
					}while(rs.next());
				}catch(Exception ex){
					System.out.println("Erro! "+ex.getMessage());
				}
				rs.close();
				stmt.close();
			}
			catch(SQLException ex){
				System.out.println("Exceção SQL: "+ex.getMessage());
			}
	}
        
        private void executeQueryGenreB(String sql){
			try{
				Statement stmt=con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
                                rs.last();
                                resultadogb=new String[rs.getRow()];
				try{
					int numCols=rs.getMetaData().getColumnCount();
					int x=0;
                                        int count=0;
                                        rs.first();
					do{
						this.resultadogb[count]=rs.getString("nm_genreb");
                                                count++;
					}while(rs.next());
				}catch(Exception ex){
					System.out.println("Erro! "+ex.getMessage());
				}
				rs.close();
				stmt.close();
			}
			catch(SQLException ex){
				System.out.println("Exceção SQL: "+ex.getMessage());
			}
	}
        
        private void executeQueryEditor(String sql){
			try{
				Statement stmt=con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
                                rs.last();
                                resultadoe=new String[rs.getRow()];
				try{
					int numCols=rs.getMetaData().getColumnCount();
					int x=0;
                                        int count=0;
                                        rs.first();
					do{
						this.resultadoe[count]=rs.getString("nm_ed");
                                                count++;
					}while(rs.next());
				}catch(Exception ex){
					System.out.println("Erro! "+ex.getMessage());
				}
				rs.close();
				stmt.close();
			}
			catch(SQLException ex){
				System.out.println("Exceção SQL: "+ex.getMessage());
			}
	}
        private void executeQueryPlace(String sql){
			try{
				Statement stmt=con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
                                rs.last();
                                resultadop=new String[rs.getRow()];
				try{
					int numCols=rs.getMetaData().getColumnCount();
					int x=0;
                                        int count=0;
                                        rs.first();
					do{
						this.resultadop[count]=rs.getString("nm_loc");
                                                count++;
					}while(rs.next());
				}catch(Exception ex){
					System.out.println("Erro! "+ex.getMessage());
				}
				rs.close();
				stmt.close();
			}
			catch(SQLException ex){
				System.out.println("Exceção SQL: "+ex.getMessage());
			}
	}
        
        private void executeQueryGeneral(String sql, String par){
			try{
				Statement stmt=con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
                                rs.last();
                                resultadogen=new String[rs.getRow()];
				try{
					int numCols=rs.getMetaData().getColumnCount();
					int x=0;
                                        int count=0;
                                        rs.first();
					do{
                                                resultadogen[count]="";
						this.resultadogen[count]=rs.getString(par);
                                                count++;
					}while(rs.next());
				}catch(Exception ex){
					System.out.println("Erro! "+ex.getMessage());
				}
				rs.close();
				stmt.close();
			}
			catch(SQLException ex){
				System.out.println("Exceção SQL: "+ex.getMessage());
			}
	}
        
        private void executeQueryStatusB(String sql){
			try{
				Statement stmt=con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
                                rs.last();
                                resultadosb=new String[rs.getRow()];
				try{
					int numCols=rs.getMetaData().getColumnCount();
					int x=0;
                                        int count=0;
                                        rs.first();
					do{
						this.resultadosb[count]=rs.getString("nm_stab");
                                                count++;
					}while(rs.next());
				}catch(Exception ex){
					System.out.println("Erro! "+ex.getMessage());
				}
				rs.close();
				stmt.close();
			}
			catch(SQLException ex){
				System.out.println("Exceção SQL: "+ex.getMessage());
			}
	}
        
        private void getID(String sql, String id){
			try{
				Statement stmt=con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				try{
					int numCols=rs.getMetaData().getColumnCount();
					int x=0;
                                        int count=0;
                                        rs.first();
					this.resultadoid=rs.getString(id);
                                        count++;
				}catch(Exception ex){
					System.out.println("Erro! "+ex.getMessage());
				}
				rs.close();
				stmt.close();
			}
			catch(SQLException ex){
				System.out.println("Exceção SQL: "+ex.getMessage());
			}
	}
        
        private void executeQueryBooks(String sql){
			try{
				Statement stmt=con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
                                rs.last();
                                resultadoBook=new String[rs.getRow()][11];
				try{
					int numCols=rs.getMetaData().getColumnCount();
					int x=0;
                                        int count=0;
                                        rs.first();
					do{
						this.resultadoBook[count][0]=rs.getString("nm_book");
                                                this.resultadoBook[count][1]=rs.getString("ori_book");
                                                this.resultadoBook[count][2]=rs.getString("wr_book");
                                                this.resultadoBook[count][3]=rs.getString("gn_book");
                                                this.resultadoBook[count][4]=rs.getString("y_book");
                                                this.resultadoBook[count][5]=rs.getString("rt_book");
                                                this.resultadoBook[count][6]=rs.getString("ed_book");
                                                this.resultadoBook[count][7]=rs.getString("pages");
                                                this.resultadoBook[count][8]=rs.getString("sta_book");
                                                this.resultadoBook[count][9]=rs.getString("localization");
                                                this.resultadoBook[count][10]=rs.getString("cover");
                                                count++;
					}while(rs.next());
				}catch(Exception ex){
					System.out.println("Erro Catch! "+ex.getMessage());
				}
				rs.close();
				stmt.close();
			}
			catch(SQLException ex){
				System.out.println("Exceção SQL: "+ex.getMessage());
			}
	}
        
        private void executeQueryFilms(String sql){
			try{
				Statement stmt=con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
                                rs.last();
                                resultadoFilm=new String[rs.getRow()][11];
				try{
					int numCols=rs.getMetaData().getColumnCount();
					int x=0;
                                        int count=0;
                                        rs.first();
					do{
						this.resultadoFilm[count][0]=rs.getString("nm_film");
                                                this.resultadoFilm[count][1]=rs.getString("dir_film");
                                                this.resultadoFilm[count][2]=rs.getString("lan_film");
                                                this.resultadoFilm[count][3]=rs.getString("olan_film");
                                                this.resultadoFilm[count][4]=rs.getString("country");
                                                this.resultadoFilm[count][5]=rs.getString("runt");
                                                this.resultadoFilm[count][6]=rs.getString("rating");
                                                this.resultadoFilm[count][7]=rs.getString("yearf");
                                                this.resultadoFilm[count][8]=rs.getString("subt");
                                                this.resultadoFilm[count][9]=rs.getString("genref");
                                                this.resultadoFilm[count][10]=rs.getString("poster");
                                                count++;
					}while(rs.next());
				}catch(Exception ex){
					System.out.println("Erro Catch! "+ex.getMessage());
				}
				rs.close();
				stmt.close();
			}
			catch(SQLException ex){
				System.out.println("Exceção SQL: "+ex.getMessage());
			}
	}
        
        private void executeQuery(String sql, String item){
		resultado="";	
                try{
				Statement stmt=con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				try{
					int numCols=rs.getMetaData().getColumnCount();
					int x=0;
                                        rs.first();
					this.resultado=rs.getString(item);
                                      
				}catch(Exception ex){
					System.out.println("Erro! "+ex.getMessage());
				}
				rs.close();
				stmt.close();
			}
			catch(SQLException ex){
				System.out.println("Exceção SQL: "+ex.getMessage());
			}
	}


	public boolean inserirDados(String url, String driver, String meuLogin, String minhaSenha, String sql){
		if(criarConexao(url, driver, meuLogin, minhaSenha)){
			boolean b;
			if(executaAtualizacao(sql)) b=true;
			else b= false;
			return b;

		}else return false;
	}

	public int deletarDados(String url, String driver, String meuLogin, String minhaSenha, String sql){
		if(criarConexao(url,driver, meuLogin, minhaSenha)){
			executaAtualizacao(sql);
			return this.numero;
		}else return -1;
	}

	public int alterDados(String url, String driver, String meuLogin, String minhaSenha, String sql){
		if(criarConexao(url,driver, meuLogin, minhaSenha)){
			executaAtualizacao(sql);
			return this.numero;
		}else return -1;
	}

	public String[] consultarDadosWriter(String url, String driver, String meuLogin, String minhaSenha, String tabela){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQueryWriter("select * from "+tabela+";");
			return resultadow;
		}else return null;
	}
        public String[] consultarDadosGenreB(String url, String driver, String meuLogin, String minhaSenha, String tabela){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQueryGenreB("select * from "+tabela+";");
			return resultadogb;
		}else return null;
	}
        public String[] consultarDadosEditor(String url, String driver, String meuLogin, String minhaSenha, String tabela){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQueryEditor("select * from "+tabela+";");
			return resultadoe;
		}else return null;
	}
        public String[] consultarDadosPlace(String url, String driver, String meuLogin, String minhaSenha, String tabela){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQueryPlace("select * from "+tabela+";");
			return resultadop;
		}else return null;
	}
        public String[] consultarDadosStatusB(String url, String driver, String meuLogin, String minhaSenha, String tabela){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQueryStatusB("select * from "+tabela+";");
			return resultadosb;
		}else return null;
	}
        
        public String[] consultarDados(String url, String driver, String meuLogin, String minhaSenha, String tabela, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQueryGeneral("select * from "+tabela+";",par);
			return resultadogen;
		}else return null;
	}
        
        public String consultWriter(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from writer where nm_writer='"+par+"';", "id_writer");
			return resultado;
		}else return null;
	}
        
        public String consultEditor(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from editor where nm_ed='"+par+"';", "id_ed");
			return resultado;
		}else return null;
	}
        
        public String consultStatusB(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from statusb where nm_stab='"+par+"';", "id_stab");
			return resultado;
		}else return null;
	}
        
        public String consultLocal(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from place where nm_loc='"+par+"';", "id_loc");
			return resultado;
		}else return null;
	}
        
        public String consultGenreB(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from genreb where nm_genreb='"+par+"';", "id_genreb");
			return resultado;
		}else return null;
	}
        
        public String consultDirector(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from director where nm_director='"+par+"';", "id_director");
			return resultado;
		}else return null;
	}
        
        public String consultLanguage(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from languages where nm_lan='"+par+"';", "id_lan");
			return resultado;
		}else return null;
	}
        
        public String consultCountry(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from country where nm_country='"+par+"';", "id_country");
			return resultado;
		}else return null;
	}
        
        public String consultSubtitles(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from subtitle where nm_subt='"+par+"';", "id_subt");
			return resultado;
		}else return null;
	}
        
        public String consultGenreF(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from genref where nm_genref='"+par+"';", "id_genref");
			return resultado;
		}else return null;
	}
        
        public String consultWriter2(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from writer where id_writer='"+par+"';", "nm_writer");
			return resultado;
		}else return null;
	}
        
        public String consultEditor2(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from editor where id_ed='"+par+"';", "nm_ed");
			return resultado;
		}else return null;
	}
        
        public String consultStatusB2(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from statusb where id_stab='"+par+"';", "nm_stab");
			return resultado;
		}else return null;
	}
        
        public String consultLocal2(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from place where id_loc='"+par+"';", "nm_loc");
			return resultado;
		}else return null;
	}
        
        public String consultGenreB2(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from genreb where id_genreb='"+par+"';", "nm_genreb");
			return resultado;
		}else return null;
	}
        
        public String consultDirector2(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from director where id_director='"+par+"';", "nm_director");
			return resultado;
		}else return null;
	}
        
        public String consultLanguage2(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from languages where id_lan='"+par+"';", "nm_lan");
			return resultado;
		}else return null;
	}
        
        public String consultCountry2(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from country where id_country='"+par+"';", "nm_country");
			return resultado;
		}else return null;
	}
        
        public String consultSubtitles2(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from subtitle where id_subt='"+par+"';", "nm_subt");
			return resultado;
		}else return null;
	}
        
        public String consultGenreF2(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from genref where id_genref='"+par+"';", "nm_genref");
			return resultado;
		}else return null;
	}
        
        public String consultComment(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from comments where book='"+par+"';", "com");
			return resultado;
		}else return null;
	}
        
        public String consultCommentF(String url, String driver, String meuLogin, String minhaSenha, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQuery("select * from commentsf where id_film='"+par+"';", "comf");
			return resultado;
		}else return null;
	}
        
        public String[][] consultBooks(String url, String driver, String meuLogin, String minhaSenha, String variable, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			executeQueryBooks("select * from books where "+variable+"='"+par+"';");
			return resultadoBook;
		}else return null;
	}
        
        public String[][] consultFilms(String url, String driver, String meuLogin, String minhaSenha, String variable, String par){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
                        executeQueryFilms("select * from films where "+variable+"='"+par+"';");
			return resultadoFilm;
		}else return null;
	}

	public boolean generalUpdate(String url, String driver, String meuLogin, String minhaSenha, String sql){
		if(criarConexao(url, driver, meuLogin, minhaSenha)){
			return(executaAtualizacao(sql));
		}else
                        return false;
	}

	public void excluirTabela(String url, String driver, String meuLogin, String minhaSenha, String tabela){
		if(criarConexao(url, driver, meuLogin, minhaSenha)){
			executaAtualizacao("drop table "+tabela);
		}
	}
        
        public String getBookID(String url, String driver, String meuLogin, String minhaSenha, String sql){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			getID(sql, "id_book");
			return resultadoid;
		}else return null;
	}
        public String getFilmID(String url, String driver, String meuLogin, String minhaSenha, String sql){
		if(criarConexao(url,driver,meuLogin, minhaSenha)){
			getID(sql, "id_film");
			return resultadoid;
		}else return null;
	}

}