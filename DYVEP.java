package cs5513;
import java.io.*;
import cs5513.MAINPROGRAM.*;   
import java.util.*;
/*
 * This code is for TPC-H Benchmark Data .
 * This contains the code for calculating affinity matrix and Clustered Affinity Matrix
 * The clustered affinity matrix is returned by this program when called from main program
 * We have taken the Bond Energy Algorithm code from the following reference :      http://read.pudn.com/downloads76/sourcecode/database/288765/BEAlgorithm2007.java__.htm
 */
public class DYVEP 
{
	   
	  private static int att = 17;
	    private static int noq =19,s;	
	    private static int[][] clustering =new int[att][att];   
	    private static int[][] clustering1 =new int[noq][att];  
	    private static int[][] attribute = new int[att][att];  
	    public static int[][] getClustering1() 
		{
			return clustering1;
		}
		public static void setClustering1(int[][] clustering1) {
			DYVEP.clustering1 = clustering1;
		}

		private static int[] sites=new int[noq];;
	    private static int index;   
	    private static int[] array;  
	    private static int b;
	           
	    private static StringTokenizer st;   
	    public static void main(String[] args) throws Exception
	    {   
	    	int i=0,j=0,k;  
	       j=0;
	    for(k=0; k<att; k++)
	    {
	        for(j=0;j<att;j++)
	        {
	        	for (i=0;i<noq;i++)
	        	{
		  // Checking the condition and computing the affinities	
	        	if(clustering1[i][k]==1 && clustering1[i][j]==1)
	        			 b+=sites[i];         	
	        	}
	                    // Calculating and assigning values to AAT
	        	attribute[k][j]=b;
	        	b=0;
	        }
	    }
	        System.out.println("\nThe attribute affinity matrix is :");
	        for (i=0;i<att;i++)
	        {
	        	for(j=0;j<att;j++)
	        	{
	        		System.out.print(attribute[i][j]+ "  ");
	        	}
	        	System.out.println();
	        }   	      	     	   	          	          	          	       	                  
	           DYVEP dy = new DYVEP();
	    	   dy.run();  
	           System.out.println("\nThe clustered affinity matrix is : ");
	           for (i=0;i<att;i++) 
	           {                  
	               for (j=0;j<att;j++)
	               {                      
	                   System.out.print(clustering[i][j] + "  ");   
	               }                  
	               System.out.println();   
	           }                      
	       }   	         
	       public static int[][] getAttribute() {
			return attribute;
		}
		public static void setAttribute(int[][] attribute) {
			DYVEP.attribute = attribute;
		}
		public static int[][] getClustering() {
			return clustering;
		}
		public static void setClustering(int[][] clustering) {
			DYVEP.clustering = clustering;
		}
		public static int[] getSites() {
			return sites;
		}
		public static void setSites(int[] sites) {
			DYVEP.sites = sites;
		}
		public void run() 
	       {          
	           array = new int[att];              
	           int loc = 0;              
	           Vector v = new Vector(); // Creates a default Vector             
	           int result = 0;              
	           for (int i=0;i<att;i++) 
	           {                
	               DYVEP.clustering[i][0] = DYVEP.attribute[i][0];                  
	               DYVEP.clustering[i][1] = DYVEP.attribute[i][1];   
	           }             
	           index = 2;              
	           int[] s = new int[3];             
	           while(index<=att-1) 
	           {               
	               array = DYVEP.attribute[index];               
	               for (int i=0;i<=index-1;i++) 
	               {                     
	                   result = this.cont(i-1,index,i);  // Returns the best placement
	                   s[0] = i-1;s[1] = index;s[2] = i;                    
	                   Union u = new Union(result, s);  
		 // Adds the element to the end of the vector and increases its size by one                   
	                   v.addElement(u);   
	               }             
	               result = this.cont(index-1, index, index+1);   
	               s = new int[3];                 
	               s[0] = index-1;s[1] = index;s[2] = index+1;                  
	               Union u = new Union(result, s);                      
	               v.addElement(u);                  
	               u = this.maxCont(v);  // Checks for the maximum contribution value             
	               s = u.getOrder();                 
	               loc = s[0]+1;                  
	               int[] temp = new int[att];          
	               for (int j=index;j>=loc;j--) 
	               {                     
	                   for (int m=0;m<att;m++) 
	                   {                       
	                       if(j-1<0)
	                       {   
	                           DYVEP.clustering[m][j] = 0;   
	                       }
	                       else                                          
	                       DYVEP.clustering[m][j] = DYVEP.clustering[m][j-1];   
	                   }                                  
	               }   
	               for (int i=0;i<DYVEP.clustering.length;i++) 
	               {                              
	                       DYVEP.clustering[i][loc] = DYVEP.attribute[i][index];   
	               }   
	               index++;         
                                  // Removes all elements from a vector and sets its size to zero         
	               v.removeAllElements();      
	           }   
	           int[] temp = new int[att];              
	           int[] tempPos = new int[att];              
	           int[][] tempC = new int[att][att];              
	           int[] original = new int[att];              
	           for (int i=0;i<att;i++) 
	           {                
	               for (int j=0;j<att;j++) 
	               {                   
	                   tempC[i][j] = DYVEP.clustering[i][j];   
	               }   
	           }             
	           for (int i=0;i<att;i++) 
	           {                
	               for (int j=0;j<att;j++) 
	               {                     
	                   temp[j] = DYVEP.clustering[j][i];                     
	               }                
	               tempPos[i] = this.checkPosV(temp);    
	           }   
	           for (int i=0;i<att;i++) 
	           {   
	               original[i] = i;   
	           }   
	           for (int i=0;i<att;i++) 
	           {                  
	               if (tempPos[i]!=original[i]) 
	               {                   
	                   int pos1 = this.checkPosH(tempC[tempPos[i]]);                    
	                   int pos2 = this.checkPosH(tempC[original[i]]);                    
	                   for (int j=0;j<att;j++) 
	                   {   
	                       DYVEP.clustering[pos1][j] = tempC[original[i]][j];                          
	                       DYVEP.clustering[pos2][j] = tempC[tempPos[i]][j];   
	                   }                   
	                   int t = original[pos1];                     
	                   original[pos1] = original[pos2];                     
	                   original[pos2] = t;                                          
	               }      
	           }         
	       }   
	        // Calculates the contribution and thus the best placement of the element  
	       public int cont(int ai, int ak, int aj) 
	       {   
	         return 2 * bond(ai,ak) + 2 * bond(ak,aj) - 2 * bond(ai,aj);   
	       }   
	        // Computes the bond value which is used to calculate the contribution of elements 
	       public int bond(int ax, int ay) 
	       {       
	           if (ax<0||ay<0||ax>att-1||ay>att-1) 
	           {                 
	               return 0;   
	           }           
	           int result = 0;           
	           if (ax==index) 
	           {                 
	               for (int i=0;i<att;i++) 
	               {             
	                   result += array[i] * DYVEP.clustering[i][ay];            
	               }              
	               return result;   
	           }        
	           if (ay==index)
	           {          
	               for (int i=0;i<att;i++) 
	               {      
	                   result += DYVEP.clustering[i][ax] * array[i];                 
	               }   
	           }           
	           for (int i=0;i<att;i++) 
	           {                 
	               result += DYVEP.clustering[i][ax] * DYVEP.clustering[i][ay];                 
	           }     
	           return result;   
	       }   
	           
	       public Union maxCont(Vector v) 
	       {   
	           // Gets the element at that position and assigng its value to 'max'
	           int max = ((Union)v.elementAt(0)).getValue();   
	           for (int i=1;i<v.size();i++)
	           {   
	               if (max < ((Union)v.elementAt(i)).getValue())
	               {   
	                   max = ((Union)v.elementAt(i)).getValue();   
	               }   
	           }   
	           for (int i=0;i<v.size();i++)
	           {   
	               if (max == ((Union)v.elementAt(i)).getValue()) 
	               {   
	                   return (Union)v.elementAt(i);   
	               }   
	           }   
	           return null;   
	       }    
	       public int checkPosV(int[] array) 
	       {   
	           boolean same = false;   
	           int[] temp = new int[att];   
	           for (int i=0;i<att;i++) 
	           {   
	               for (int j=0;j<att;j++) 
	               {   
	                  temp[j] = DYVEP.attribute[j][i];   
	               }        
	               for (int k=0;k<att;k++)
	               {   
	                   if (array[k]==temp[k]) 
	                   {   
	                       same = true;   
	                       continue;        
	                   }
	                   else
	                   {   
	                       same = false;      
	                       break;   
	                   }   
	               }   
	               if (same == true) return i;    
	           }   
	           return -1;   
	       }   
	       
	       public int checkPosH(int[] array) 
	       {          
	           boolean same = false;   
	           for (int i=0;i<att;i++)
	           {   
	               for (int j=0;j<att;j++) 
	               {   
	                   if (array[j] == DYVEP.clustering[i][j]) 
	                   {    
	                       same = true;    
	                       continue;      
	                   }
	                   else
	                   {   
	                       same = false;   
	                       break;   
	                   }   
	               }              
	               if (same == true) return i;   
	           }   
	           return -1;   
	       }   
	    
	       class Union 
	       {          
	           private int contValue;   
	           private int[] ordering;   
	              
	           public Union(int v, int[] s) 
	           {   
	               this.contValue = v;   
	               this.ordering = s;   
	           }   
	           public int[] getOrder() 
	           {   
	               return this.ordering;   
	           }   
	           public int getValue() 
	           {   
	               return this.contValue;   
	           }   
	       }    	    
}
