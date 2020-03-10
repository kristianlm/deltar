;;; one way incremental files sync
(import (only chicken.string conc reverse-list->string)
        (only chicken.condition print-error-message condition->list condition-case)
        (only chicken.file file-exists? create-directory directory delete-file move-file file-readable?)
        (only chicken.file.posix directory? file-type file-modification-time file-size read-symbolic-link)
        (only chicken.port make-output-port port-for-each with-input-from-string)
        (only chicken.random pseudo-random-integer)
        (only chicken.blob blob->string blob?)
        (only chicken.pathname decompose-pathname make-pathname absolute-pathname? normalize-pathname)
        (only chicken.process-context command-line-arguments argv get-environment-variable current-directory)
        (only chicken.process with-input-from-pipe)
        (only chicken.io read-string read-string!)
        (only chicken.sort sort)
        ;; (prefix (hash sha1) sha1.) using openssl's instead, almost twice as fast
        (prefix libcrypto.sha1 sha1.)
        (only matchable match match-lambda*)
        fmt ;; <-- for metric prefixes
        srfi-4 chicken.foreign
        srfi-69)

;; obs: not thread-safe
(define (make-counter #!optional (count 0))
  (case-lambda
   (() count)
   ((n) (set! count (+ n count)) count)))

(define seen+      (make-counter))
(define dirs+      (make-counter))
(define files+     (make-counter))
(define links+     (make-counter))

(define seenbytes+ (make-counter))
(define totabytes+ (make-counter))

(define (type path)
  (case (file-type path #t)
    ((regular-file)     'f) ((directory)        'd) ((fifo)             'p)
    ((socket)           's) ((symbolic-link)    'l) ((character-device) 'c)
    ((block-device)     'b)))

;; slow implementation of #${abba} => "abba"
(define (blob->hex blob)
  (define (hex->char i) (string-ref "0123456789abcdef" i))
  (reverse-list->string
   (foldl
    (lambda (sum x)
      (let* ((c (char->integer x))
             (nibble0 (hex->char (quotient c 16)))
             (nibble1 (hex->char (modulo   c 16))))
        (cons nibble1 (cons nibble0 sum))))
    '()
    (string->list (blob->string blob)))))

(define (hex->blob str)
  ;; TODO: don't tell anyone we've done it like this
  (with-input-from-string (conc "#${" str "}") read))

;; alphabetically, files first, then directories
(define (sort-paths paths)
  (let loop ((paths paths)
             (files '())
             (dirs  '()))
    (if (pair? paths)
        (let ((path (car paths)))
          (if (directory? path)
              (loop (cdr paths) files (cons path dirs))
              (loop (cdr paths) (cons path files) dirs)))
        (begin ;;map (lambda (p) (make-pathname (list dir p) #f))
          (append (sort files string<?) (sort dirs string<?))))))

(define .deltar/ (make-pathname (list (or (get-environment-variable "HOME") "./") ".deltar/") #f))
(define .deltar/tmp   (make-pathname (list .deltar/ "tmp") #f))
(define .deltar/blobs (make-pathname (list .deltar/ "blobs") #f))
(define .deltar/sha1  (make-pathname (list .deltar/ "sha1") #f))
(create-directory .deltar/tmp   #t)
(create-directory .deltar/blobs #t)
(create-directory .deltar/sha1  #t)

(define (temporary-pathname)
  (create-directory .deltar/tmp #t)
  (make-pathname .deltar/tmp (number->string (pseudo-random-integer #xffffffff) 16)))

;; path of cache file which hopefully will already contain the correct
;; hash-sum.
(define (hashcache-path path)
  (receive (dir file extension) (decompose-pathname (normalize-pathname path))
    (make-pathname (append (list .deltar/sha1) (if dir (list dir) '()))
                   (conc (make-pathname #f file extension) ":" (file-modification-time path)))))

;; path of file which contains raw data, typically directory
;; contents. its filename is the hash itself.
;; (blob->path #${abba})
;; TODO: two first characters into a directory to avoid flooding a single dir.
(define (blob->path hash)
  (make-pathname .deltar/blobs (blob->hex hash)))

;; like `sha1sum $path` but without subprocess spawning. it's very
;; fast since we're using the same underlying SHA1 implementation from
;; openssl.
(define (path->sha1sum path)
  (let ((ctx (sha1.init))
        (buffer (make-string (* 4 1024 1024)))) ;; <-- 4MiB chunks
    (with-input-from-file path
      (lambda ()
        (port-for-each (lambda (len) (sha1.update! ctx buffer len))
                       (lambda () (let ((len (read-string! #f buffer)))
                                    (if (zero? len) #!eof len))))))
    (sha1.final! ctx)))

;; read hash from index if exists, or create index if needed. this
;; relies on mtime, heavily. for the hashcache-hit, it needs to match
;; on mtime too. old hashes are currently kept for no known reason.
(define (hashcache! pathO)
  (let ((pathI (hashcache-path pathO)))
    (if (file-exists? pathI)
        (begin
          (seen+ 1) (seenbytes+ (file-size pathO))
          (with-input-from-file pathI read))
        ;;                                                                     ,-- skip path part
        (let ((hash (path->sha1sum pathO)))
          (receive (dir file extension) (decompose-pathname pathI)
            (create-directory dir #t))
          (with-output-to-file pathI (lambda () (write hash)))
          hash))))

;; recurse into directory path and start getting hashes of things
;; inside it. these hashes are written to a tmp-file, which upon
;; successful completion is renamed to match the sha1sum of its
;; content. this is very much the core of our blob-store.
;;
;; it is also very recursive. if you have 10 levels of nested
;; directories, you'll have 10 open files as you process them.
(define (hash-dir path callback)
  (let* ((ctx (sha1.init))
         (./tmp (temporary-pathname))
         (tmp* (open-output-file ./tmp #:binary)) ;; just to file
         (tmp (make-output-port (lambda (str) ;; to file and hash ctx
                                  (sha1.update! ctx str)
                                  (display str tmp*))
                                (lambda () #f))))
    ;; print directory contents into tmp, recursively
    (for-each
     (lambda (path) ;; <-- could be file or directory
       (let* ((h (snapshot path callback)))
         ;; this output is written to parent dir's content list
         (write (list (type path) path h) tmp)
         (newline tmp)))
     (map (lambda (f) (make-pathname path f)) (directory path)))
    (close-output-port tmp*)
    (let* ((h (sha1.final! ctx))
           (path (blob->path h)))
      (if (file-exists? path) ;; <-- identical directory content already exists (empty?)
          (begin
            (seen+ 1)
            (delete-file ./tmp))
          (move-file ./tmp path))
      h)))

;; return the atomic representation of file/directory on path. this is
;; the hash-value of the contents for files and directories, and the
;; target for symbolic-links.
(define (snapshot path #!optional
                  (callback
                   (lambda (type path hash)
                     (print* "\r" (dirs+) " dirs " (files+) " files, "
                             "seen " (seen+) "/" (+ (dirs+) (files+) (links+)) " "
                             (if (zero? (seenbytes+)) "0B" (fmt #f (num/si (seenbytes+) 1024 "B"))) " / "
                             (if (zero? (totabytes+)) "0B" (fmt #f (num/si (totabytes+) 1024 "B"))) " "))))
  (let ((path (normalize-pathname ;; <-- important! (sha1sum "/home/user") ≠ (sha1sum "/home/./user")
               (if (absolute-pathname? path) path
                   (make-pathname (list (current-directory) path) #f)))))
   (condition-case
    (if (file-readable? path)
        (let* ((T (type path))
               (result (case T
                         ((d) (dirs+ 1)  (hash-dir path callback))
                         ((f) (files+ 1) (totabytes+ (file-size path)) (hashcache! path))
                         ((l) (links+ 1) (read-symbolic-link path))
                         (else (print "warning: skipping " (file-type path) " " path)))))
          (callback T path result)
          result)
        (print "warning: no permissions on " (file-type path) " " path))
    (e (i/o) (print (condition->list e)) (print-error-message e)))))

;; traverse snapshot recursively with callbacks
(define (traverse snapshot callback)
  (with-input-from-file (blob->path (if (blob? snapshot) snapshot (hex->blob snapshot)))
    (lambda ()
      (port-for-each
       (lambda (item)
         (apply callback item)
         ;; directories are recursive
         (match item
           (('d path hash) (traverse hash callback))
           (else)))
       read))))

(define (tree snapshot)
  (traverse snapshot
            (match-lambda*
             (('f path hash) (print (blob->hex hash) " " path))
             (('d path hash) (print (blob->hex hash) " " (make-pathname path #f) ))
             (else (print "TODO match " else)))))

(define (delta snapshot)
  (let ((ht (make-hash-table)))
    (print* "analysing snapshot ...") (flush-output)
    (traverse snapshot (lambda (type path hash) (hash-table-set! ht hash #t)))
    (print "done")
    (snapshot "."
              (lambda (type path hash)
                (let ((present? (hash-table-ref ht hash (lambda () #f))))
                  (match type
                    ('l (print "TODO: symlinks"))
                    ('f (unless present? (print "mv " (blob->path hash) " " path)))
                    ('d (unless present? (print "echo d " hash  " " path)))
                    (else (print "TODO: " type))))))))

(define (main args)
  (define (header)
    (print "using repo " .deltar/))
  (define (usage)
    (print "usage: deltar [cmd ...]
where cmd ... is:

  snapshot [directory]
    Snapshot files and directories recursively
    to produce a snapshot hash.

  tree <snapshot>
    Print the list of files for <snapshot>.

version " (let-syntax ;; compile in version string
      ((git-tag
        (er-macro-transformer
         (lambda (x r t)
           (import (only chicken.process with-input-from-pipe)
                   (only chicken.io read-line))
           (with-input-from-pipe "git describe --tags --always --dirty" read-line)))))
    (git-tag))))

  (match args
    (() (usage))
    (("snapshot") (main `("snapshot" ".")))
    (("snapshot" dir)
     (header)
     (let ((h (snapshot dir)))
       (newline)
       (print h)))
    (("tree" snapshot)
     (header)
     (tree snapshot))
    (("delta" snapshot)
     (header)
     (delta snapshot))
    (else (print "unknown command " args))))

(main (command-line-arguments))

;; - [x] fix cached results ≠ noncached results!
;; - [ ] fix symlinks that point nowhere, they produce #<unspecified> hashes
;; - [ ] colorize output using LS_COLORS
;; - [ ] persist snapshots
;; - [ ] list snapshots
