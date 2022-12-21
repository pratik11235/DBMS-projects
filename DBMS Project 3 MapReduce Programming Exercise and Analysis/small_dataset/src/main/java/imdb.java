import java.io.*;
import java.util.*;

import javax.print.DocFlavor.STRING;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


class Actor implements Writable {
    public String tid;
    public String aid;
    public String aname;
	
    Actor () {}
	
    Actor ( String tid, String aid, String aname ) { this.tid = tid; this.aid = aid; this.aname = aname; }
	
    public void write ( DataOutput out ) throws IOException {
        out.writeUTF(tid);
		out.writeUTF(aid);
        out.writeUTF(aname);
    }

    public void readFields ( DataInput in ) throws IOException {
        tid = in.readUTF();
		aid = in.readUTF();
        aname = in.readUTF();
    }
}

class Director implements Writable {
    public String tid;
    public String did;
	
    Director () {}
	
    Director ( String tid, String did) { this.tid = tid; this.did = did; }
	
    public void write ( DataOutput out ) throws IOException {
        out.writeUTF(tid);
		out.writeUTF(did);
    }

    public void readFields ( DataInput in ) throws IOException {
        tid = in.readUTF();
		did = in.readUTF();
    }
}

class Movie implements Writable {
    public String tid;
    public String mname;
    public String genre;
    public String year;
    public String ttype;
	
    Movie () {}
	
    Movie ( String tid, String mname, String genre, String year, String ttype) { this.tid = tid; this.mname = mname; this.genre = genre; this.year = year; this.ttype = ttype;}
	
    public void write ( DataOutput out ) throws IOException {
        out.writeUTF(tid);
		out.writeUTF(mname);
        out.writeUTF(genre);
        out.writeUTF(year);
        out.writeUTF(ttype);
    }

    public void readFields ( DataInput in ) throws IOException {
        tid = in.readUTF();
		mname = in.readUTF();
        genre = in.readUTF();
        year = in.readUTF();
        ttype = in.readUTF();
    }
}

  class MAD implements Writable {
      public Actor actor;
      public Director director;
      public Movie movie;
      public int flag;
      
      MAD () {}

      MAD ( Actor a, int flag ) { this.actor = a; this.flag = flag; }
      MAD ( Director d, int flag ) { this.director = d; this.flag = flag; }
      MAD ( Movie m, int flag ) { this.movie = m; this.flag = flag; }

      public void write ( DataOutput out ) throws IOException {
        out.writeInt(flag);
        if (flag==1){
            System.out.println("Write"+actor.tid+" "+flag);
            this.actor.write(out);}
        else if(flag==2)
            this.director.write(out);
        else if(flag==3)
            this.movie.write(out);
    }

    public void readFields ( DataInput in ) throws IOException {
        flag = in.readInt();
        if (flag==1) {
            actor = new Actor();
            actor.readFields(in);
            System.out.println("read"+actor.tid+" "+flag);} 
        else if (flag==2) {
            director = new Director();
            director.readFields(in);
        }
        else if(flag==3) {
            movie = new Movie();
            movie.readFields(in);
        }
    }
  }

  class Output implements Writable {
    public String aname;
    public String ttype;
    public String year;
    public String mname;

    Output () {}

    Output ( String an, String tt, String y, String mn ) {
        aname = an;
        ttype = tt;
        year = y;
        mname = mn;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeUTF(aname);
        out.writeUTF(ttype);
        out.writeUTF(year);
        out.writeUTF(mname);

    }

    public void readFields ( DataInput in ) throws IOException {
        aname = in.readUTF();
        ttype = in.readUTF();
        year = in.readUTF();
        mname = in.readUTF();
    }

    public String toString () { return ttype+" "+year+" "+aname+" "+mname; }
}

public class imdb  {

    public static class Actor_Mapper extends Mapper<Object,Text,Text,MAD> {
            @Override
            public void map ( Object key, Text value, Context context )
                            throws IOException, InterruptedException {
                
                Scanner s = new Scanner(value.toString()).useDelimiter(",");
                String tid = s.next();
                String aid = s.next();                
                String aname = s.next();
                int flag =1;
                Actor a = new Actor(tid, aid, aname);
                context.write(new Text(tid), new MAD(a,flag));
                s.close();
            }
        }

        public static class Director_Mapper extends Mapper<Object,Text,Text,MAD> {
            @Override
            public void map ( Object key, Text value, Context context )
                            throws IOException, InterruptedException {
                //Scanner dids = null;
                Scanner s = new Scanner(value.toString()).useDelimiter("\t");
                String tid = s.next();
                String [] did = s.next().split(",");   
                String w = s.next();
                int flag = 2;
                if(did.length>1){
                    for(String s1 : did){
                        Director d = new Director(tid, s1);
                context.write(new Text(tid), new MAD(d,flag));
                    }
                }             
                else{
                Director d = new Director(tid, did[0]);
                //System.out.println(d.tid+" "+d.did);
                context.write(new Text(tid), new MAD(d,flag));
                s.close();
                }
        }
    }

        public static class Movie_Mapper extends Mapper<Object,Text,Text,MAD> {
            @Override
            public void map ( Object key, Text value, Context context )
                            throws IOException, InterruptedException {
                
                Scanner s = new Scanner(value.toString()).useDelimiter("\t");
                String tid = s.next();
                String type = s.next();
                String mname = s.next();
                String oname = s.next();
                String adult = s.next();
                String year = s.next();
                String eyear = s.next();
                String run = s.next();                
                String genre = s.next();
                int flag =3;
                if(year.equals("\\N")){
                    year = "0";
                }
                //System.out.println("Inside Movie mApper");
                Movie m = new Movie(tid, mname, genre, year, type);
                context.write(new Text(tid), new MAD(m,flag));
                s.close();
            }
        }


    public static class MyReducer extends Reducer<Text,MAD,Text,Text> {
        static Vector<Actor> act = new Vector<Actor>();
        static Vector<Director> dir = new Vector<Director>();
        static Vector<Movie> mov = new Vector<Movie>();
            @Override
            public void reduce (Text key, Iterable<MAD> values, Context context )
                               throws IOException, InterruptedException {
                act.clear();
                dir.clear();
                mov.clear();
                for ( MAD v: values){
                    System.out.println("THIS IS FLAG"+v.flag+v.actor.aid);
                    if(v.flag==1)
                    act.add(v.actor);
                        //System.out.println("REducer ACtor"+v.actor.tid);}
                    else if(v.flag==2)
                    dir.add(v.director);
                    else if(v.flag==3)
                    mov.add(v.movie);
                }
                //System.out.println("Inside REducer ");
                    //System.out.println("This is the vector"+act.size());
                for( Actor a: act){
                    for( Director d: dir){
                        for( Movie m: mov){
                            if((Integer.parseInt(m.year)>=1963&&Integer.parseInt(m.year)<=1973&&m.ttype.equals("movie"))||(m.year.equals("0")&&m.ttype.equals("movie"))){
                                if(a.aid.equals(d.did)){
                                    if(a.tid.equals(m.tid))
                                    context.write(key, new Text(a.aname+" "+m.ttype+" "+m.year+" "+m.mname));
                                }
                            }
                        }
                    }
                }
            }
        }    
   
        
    public static void main ( String[] args ) throws Exception {
        //System.out.println("Inside Main");
    Configuration conf = new Configuration();
    int split = 700*1024*1024; // This is in bytes
    String splitsize = Integer.toString(split);
    conf.set("mapreduce.input.fileinputformat.split.minsize",splitsize);
    // conf.set("mapreduce.map.memory.mb", "2048"); // This is in Mb
    // conf.set("mapreduce.reduce.memory.mb", "2048"); 
    Job job1 = Job.getInstance(conf, "actor-director gig");
    job1.setJarByClass(imdb.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(MAD.class);
    job1.setReducerClass(MyReducer.class);
    job1.setOutputFormatClass(TextOutputFormat.class);
    MultipleInputs.addInputPath(job1,new Path(args[0]), TextInputFormat.class, Movie_Mapper.class);
    MultipleInputs.addInputPath(job1,new Path(args[1]), TextInputFormat.class, Actor_Mapper.class);
    MultipleInputs.addInputPath(job1,new Path(args[2]), TextInputFormat.class, Director_Mapper.class);
    FileOutputFormat.setOutputPath(job1, new Path(args[3]));
    job1.waitForCompletion(true);
    //job1.setInputFormatClass(TextInputFormat.class);
    // job1.setMapperClass(Movie_Mapper.class);
    // job1.setMapperClass(Actor_Mapper.class);
    // job1.setMapperClass(Director_Mapper.class);
   
    }
}