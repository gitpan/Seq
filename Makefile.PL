use Inline::MakeMaker;
WriteInlineMakefile(
    NAME              => 'Seq',
    VERSION_FROM      => 'Seq.pm', # finds $VERSION
    PREREQ_PM         => { FileHandle => 0.01, # actually later
                           DBI => 0.01,
                           'DBD::SQLite' => 0.01,
						   Inline => 0.43 }, 
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'Seq.pm', # retrieve abstract from module
       AUTHOR         => 'Ira Woodhead <ira at sweetpota dot to>') : ()),
);
