FROM scratch
ADD x86_64/1ebe9859248159d10557a6477c2f2127ed14b108cc74602b26cbfba4fa38b47d.tar.xz /
ADD x86_64/457b39c0faea15210bc3407fe51fc86b5c1d40664886700f45849bee7bd4904a.tar.xz /
ADD x86_64/7829ac41fab0373bf7e054ee844a8b5016a7a3ac054c320610febadc7208831b.tar.xz /
ADD x86_64/92cd97bbdd8f58b619302cf7571b1ea1b75c83aaf95666a49f8a21053f9af149.tar.xz /
ADD x86_64/ede9fc727b406d8b24c12a01ee37a606fd966a32f8dcd87ed73c189caad85b8e.tar.xz /
ADD x86_64/fae23ff1a6e1d5f1d5e33e2e1b6dcf13709b9f9fff5e4f5acb62cfb5019b236f.tar.xz /

ENV LANG=en_US.UTF-8
ENV TZ=:/etc/localtime
ENV PATH=/var/lang/bin:/usr/local/bin:/usr/bin/:/bin:/opt/bin
ENV LD_LIBRARY_PATH=/var/lang/lib:/lib64:/usr/lib64:/var/runtime:/var/runtime/lib:/var/task:/var/task/lib:/opt/lib
ENV LAMBDA_TASK_ROOT=/var/task
ENV LAMBDA_RUNTIME_DIR=/var/runtime

WORKDIR /var/task

ENTRYPOINT ["/lambda-entrypoint.sh"]

FROM public.ecr.aws/lambda/python:3.8

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]