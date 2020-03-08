;;; one way incremental files sync
(import chicken.string chicken.file chicken.file.posix  chicken.port
        chicken.random
        chicken.blob chicken.pathname  chicken.process
        chicken.process-context chicken.io chicken.sort
        (prefix (hash sha1) sha1:)
        srfi-4 chicken.foreign
        simple-sha1)

;; obs: not thread-safe
(define (make-counter #!optional (count 0))
  (case-lambda
   (() count)
   ((n) (set! count (+ n count)) count)))

(define seen+ (make-counter))
(define dirs+ (make-counter))
(define files+ (make-counter))

(define (type path)
  (case (file-type path)
    ((regular-file)     'f)
    ((directory)        'd)
    ((fifo)             'p)
    ((socket)           's)
    ((symbolic-link)    'l)
    ((character-device) 'c)
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

(define .deltar ".deltar")
(create-directory (make-pathname .deltar "tmp")   #t)
(create-directory (make-pathname .deltar "blobs") #t)
(create-directory (make-pathname .deltar "index") #t)

(define (temporary-pathname)
  (create-directory (make-pathname (list .deltar "tmp") #f) #t)
  (make-pathname (list .deltar "tmp")
                 (conc (pseudo-random-integer #xffffffff))))

(define (touch path)
  (file-close (file-open path (+ open/write open/creat))))

(define (path->index path)
  (receive (dir file extension)
      (decompose-pathname (normalize-pathname path))
    ;;(print "DIR " dir " path " path)
    (make-pathname (append (list .deltar "index") (if dir dir '()))
                   (conc (make-pathname #f file extension) ":" (file-modification-time path)))))

(define (blob->path hash)
  (make-pathname (list .deltar "blobs") (blob->hex hash)))

;; read hash from index if exists, or create index if needed. this
;; relies on mtime, heavily. for the hashcache-hit, it needs to match
;; on mtime too. old hashes are currently kept for no known reason.
(define (hashcache! pathO)
  (let ((pathI (path->index pathO)))
    (if (file-exists? pathI)
        (begin
          (seen+ 1)
          (with-input-from-file pathI read-string))
        ;;                                                                     ,-- skip path part
        (let ((hash (conc (with-input-from-pipe (conc "sha1sum '" pathO "'") read))))
          (receive (dir file extension) (decompose-pathname pathI)
            (create-directory dir #t))
          (with-output-to-file pathI (lambda () (display hash)))
          hash))))

;; recurse into directory path and start getting hashes of things
;; inside it. these hashes are written to a tmp-file, which upon
;; successful completion is renamed to match the sha1sum of its
;; content. this is very much the core of our blob-store.
(define (hash-dir path)
  (let* ((ctx (sha1:init))
         (./tmp (temporary-pathname))
         (tmp* (open-output-file ./tmp #:binary)) ;; just to file
         (tmp (make-output-port (lambda (str) ;; to file and hash ctx
                                  (sha1:update! ctx str)
                                  (display str tmp*))
                                (lambda () #f))))
    ;; copy output into hash
    (for-each
     (lambda (path) ;; <-- could be file or directory
       (let* ((h (hash path)))
         (write (list (type path) path h) tmp) (newline tmp)))
     (sort-paths (map (lambda (f) (make-pathname path f)) (directory path))))
    (close-output-port tmp*)
    (let* ((h (sha1:final! ctx))
           (path (blob->path h)))
      (if (file-exists? path)
          (begin
            (seen+ 1)
            (delete-file ./tmp)) ;; blob already exists (da39a3ee5e6b4b0d3255bfef95601890afd80709 is popular)
          (move-file ./tmp path))
      h)))

(define (hash path)
  (let ((result (case (type path)
                  ((d) (dirs+ 1)  (hash-dir path))
                  ((f) (files+ 1) (hashcache! path))
                  (else (error "TODO: " (type path) " for " path)))))
    (print* "\rseen " (seen+) "/" (+ (dirs+) (files+)) " " (dirs+) " dirs " (files+) " files ")
    result))

(let ((h (hash ".")))
  (newline)
  (print h))
