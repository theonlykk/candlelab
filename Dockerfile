FROM python:3.11-slim

RUN useradd -m -u 1000 user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

WORKDIR $HOME/app

COPY --chown=user:user requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=user:user . .

RUN mkdir -p /home/user/data && chown -R user:user /home/user/data

ENV PORT=7860

EXPOSE 7860
USER user

# v4: init DB only, gunicorn serves, backfill on-demand per instrument
CMD bash -c "python -c 'from data import init_db; init_db()' && exec gunicorn -b 0.0.0.0:7860 --workers 1 --threads 4 --timeout 120 app:app"