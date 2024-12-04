import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

class DBManager:
    def __init__(self, db_path: str = "papers.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database and create tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # Create papers table
        c.execute("""
            CREATE TABLE IF NOT EXISTS papers (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                authors TEXT NOT NULL,
                abstract TEXT,
                source TEXT NOT NULL,
                publication_date TEXT NOT NULL,
                access_date TEXT NOT NULL,
                pdf_path TEXT,
                bibtex_path TEXT,
                endnote_path TEXT,
                is_paywalled BOOLEAN DEFAULT FALSE,
                summary TEXT,
                implications TEXT,
                topic_category TEXT,
                error_log TEXT
            )
        """)
        
        # Create tags table
        c.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            )
        """)
        
        # Create paper_tags table
        c.execute("""
            CREATE TABLE IF NOT EXISTS paper_tags (
                paper_id TEXT,
                tag_id INTEGER,
                PRIMARY KEY (paper_id, tag_id),
                FOREIGN KEY (paper_id) REFERENCES papers(id),
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            )
        """)
        
        # Create indices
        c.execute("CREATE INDEX IF NOT EXISTS idx_papers_publication_date ON papers(publication_date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_papers_source ON papers(source)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_papers_topic ON papers(topic_category)")
        
        conn.commit()
        conn.close()
    
    def add_paper(self, paper: Dict) -> bool:
        """Add a new paper to the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            c.execute("""
                INSERT INTO papers (
                    id, title, authors, abstract, source,
                    publication_date, access_date, pdf_path,
                    bibtex_path, endnote_path, is_paywalled,
                    summary, implications, topic_category
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                paper['id'],
                paper['title'],
                ','.join(paper['authors']),
                paper.get('abstract'),
                paper['source'],
                paper['publication_date'],
                datetime.now().isoformat(),
                paper.get('pdf_path'),
                paper.get('bibtex_path'),
                paper.get('endnote_path'),
                paper.get('is_paywalled', False),
                paper.get('summary'),
                paper.get('implications'),
                paper.get('topic_category')
            ))
            
            conn.commit()
            return True
            
        except sqlite3.Error as e:
            print(f"Error adding paper: {e}")
            return False
            
        finally:
            conn.close()
    
    def get_paper(self, paper_id: str) -> Optional[Dict]:
        """Retrieve a paper by ID"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute("SELECT * FROM papers WHERE id = ?", (paper_id,))
        paper = c.fetchone()
        
        if paper:
            columns = [description[0] for description in c.description]
            paper_dict = dict(zip(columns, paper))
            paper_dict['authors'] = paper_dict['authors'].split(',')
            
            # Get tags
            c.execute("""
                SELECT t.name 
                FROM tags t 
                JOIN paper_tags pt ON t.id = pt.tag_id 
                WHERE pt.paper_id = ?
            """, (paper_id,))
            
            paper_dict['tags'] = [row[0] for row in c.fetchall()]
            return paper_dict
            
        return None

    def search_papers(self, query: str, source: Optional[str] = None, 
                     from_date: Optional[str] = None) -> List[Dict]:
        """Search papers by query, optionally filtered by source and date"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        sql = """
            SELECT * FROM papers 
            WHERE (title LIKE ? OR abstract LIKE ?)
        """
        params = [f"%{query}%", f"%{query}%"]
        
        if source:
            sql += " AND source = ?"
            params.append(source)
        
        if from_date:
            sql += " AND publication_date >= ?"
            params.append(from_date)
            
        c.execute(sql, params)
        papers = []
        
        for row in c.fetchall():
            columns = [description[0] for description in c.description]
            paper_dict = dict(zip(columns, row))
            paper_dict['authors'] = paper_dict['authors'].split(',')
            
            # Get tags
            c.execute("""
                SELECT t.name 
                FROM tags t 
                JOIN paper_tags pt ON t.id = pt.tag_id 
                WHERE pt.paper_id = ?
            """, (paper_dict['id'],))
            
            paper_dict['tags'] = [row[0] for row in c.fetchall()]
            papers.append(paper_dict)
            
        conn.close()
        return papers

    def add_tags(self, paper_id: str, tags: List[str]):
        """Add tags to a paper"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        try:
            for tag in tags:
                # Insert tag if it doesn't exist
                c.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag,))
                
                # Get tag id
                c.execute("SELECT id FROM tags WHERE name = ?", (tag,))
                tag_id = c.fetchone()[0]
                
                # Link tag to paper
                c.execute("""
                    INSERT OR IGNORE INTO paper_tags (paper_id, tag_id) 
                    VALUES (?, ?)
                """, (paper_id, tag_id))
            
            conn.commit()
            
        except sqlite3.Error as e:
            print(f"Error adding tags: {e}")
            conn.rollback()
            
        finally:
            conn.close()

    def update_paper(self, paper_id: str, updates: Dict) -> bool:
        """Update paper fields"""
        valid_fields = {
            'title', 'authors', 'abstract', 'pdf_path', 'bibtex_path',
            'endnote_path', 'is_paywalled', 'summary', 'implications',
            'topic_category', 'error_log'
        }
        
        update_fields = []
        values = []
        
        for field, value in updates.items():
            if field in valid_fields:
                update_fields.append(f"{field} = ?")
                values.append(value if field != 'authors' else ','.join(value))
        
        if not update_fields:
            return False
            
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            sql = f"""
                UPDATE papers 
                SET {', '.join(update_fields)}
                WHERE id = ?
            """
            values.append(paper_id)
            
            c.execute(sql, values)
            conn.commit()
            return True
            
        except sqlite3.Error as e:
            print(f"Error updating paper: {e}")
            return False
            
        finally:
            conn.close()
