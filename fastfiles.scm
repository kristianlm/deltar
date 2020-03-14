;;; CHICKEN's default (directory ".") is nice and convenient: It
;;; returns an array of strings. However, we want to specialize for
;;; speed. By providing this for-each, we don't need to do two system
;;; calls to get the file type (dir/file) because it's passed to us in
;;; the readdir struct.
;;;
;;; What's nice is that with this we're on par with find:
;;;    $ ./files-test | pv -l > /dev/null
;;;    918k 0:00:01 [ 591k/s]
;;;    $ find | pv -l > /dev/null
;;;    932k 0:00:01 [ 579k/s]
;;;
;;; TODO: don't error out on "Permission denied"
(module fastfiles (directory-for-each)

(import scheme chicken.base
        chicken.foreign
        chicken.pathname)

(foreign-declare "
#include <sys/types.h>
#include <dirent.h>
")

(define opendir (foreign-lambda (c-pointer "DIR") "opendir" c-string))
(define (readdir dir)
  (let ((dirent ((foreign-lambda* (c-pointer (struct "dirent"))
                                  (((c-pointer "DIR") dir))
                                  "errno = 0;"
                                  "return(readdir(dir));") dir)))
    (if (= (foreign-value "errno" int) 0)
        dirent
        (error "unexpected readdir error: " (foreign-value "errno" int)))))
(define closedir (foreign-lambda int "closedir" (c-pointer "DIR")))

(define (dirent-type dir)
  (let ((n ((foreign-lambda* unsigned-byte ( ((c-pointer (struct "dirent")) x) )
                             "return(x->d_type);") dir)))
    ;; single-letter conventions from `find -type n`:
    (cond ((= n (foreign-value "DT_BLK"     int)) 'b)
          ((= n (foreign-value "DT_CHR"     int)) 'c)
          ((= n (foreign-value "DT_DIR"     int)) 'd)
          ((= n (foreign-value "DT_FIFO"    int)) 'p)
          ((= n (foreign-value "DT_LNK"     int)) 'l)
          ((= n (foreign-value "DT_REG"     int)) 'f)
          ((= n (foreign-value "DT_SOCK"    int)) 's)
          ((= n (foreign-value "DT_UNKNOWN" int)) #f))))

(define (dirent-name dir)
  (let* ((len ((foreign-lambda* int ( ((c-pointer (struct "dirent")) x))
                                "return(strlen(x->d_name));") dir))
         (buf (make-string len)))
    ((foreign-lambda* void ( ((c-pointer (struct "dirent")) x)
                             (scheme-pointer buf)
                             (size_t len))
                      "memcpy(buf, x->d_name, len);")
     dir buf len)
    buf))

;; OBS: skips all dotfiles
(define (directory-for-each proc path0)

  (define (directory-for-each* proc* path)
    (let ((dir (opendir path)))
      (if dir
          (dynamic-wind
              (lambda () #f)
              (lambda ()
                (let loop ()
                  (let ((dirent (readdir dir)))
                    (unless (eq? #f dirent)
                      (let ((name (dirent-name dirent))
                            (type (dirent-type dirent)))
                        (unless (eq? #\. (string-ref name 0))
                          (proc* type name)))
                      (loop)))))
              (lambda () (closedir dir)))
          (error "error reading directory " path dir))))

  (let loop ((path path0))
    (directory-for-each* (lambda (type rpath)
                           (let ((apath (if (eq? type 'd)
                                            (make-pathname (list path rpath) #f)
                                            (make-pathname path rpath))))
                             (if (and (proc type apath) (eq? type 'd))
                                 (loop apath))))
                         path)))

)
