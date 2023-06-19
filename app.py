import networkx as nx
import numpy as np
import time
import plotly
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import json
import os
import kafka
from kafka import KafkaConsumer, TopicPartition
from flask import Blueprint, render_template
from flask import Flask

app = Flask(__name__)

bp = Blueprint(name="/", import_name=__name__, url_prefix="/")

@bp.route('/')
def index():
    c = render_template('pages/index.html')
    return c


@bp.route('/chart1')
def chart1():
    consumer = KafkaConsumer(
        client_id="test-docker",
        bootstrap_servers=['kafka.secview:9092'],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset='latest',
        enable_auto_commit=False,
        consumer_timeout_ms=3000
    )

    consumer.partitions_for_topic("pcap")
    partition = TopicPartition(topic="pcap", partition=0)
    consumer.assign(partitions=[partition])
    consumer.seek(partition=partition, offset=0)

    for i in range(5):
        msg = consumer.poll(
            max_records=2000, timeout_ms=2500, update_offsets=True)
        if msg:
            break

    aaa = list()

    for i in range(len(msg[partition])):
        aaa.append(msg[partition][i][6])

    df = pd.DataFrame(aaa)
    labels = list(
        np.unique(np.append(df.SrcMacAddress.unique(), df.DstMacAddress.unique())))
    df["SrcMacAddressId"] = df.SrcMacAddress.apply(lambda x: labels.index(x))
    df["DstMacAddressId"] = df.DstMacAddress.apply(lambda x: labels.index(x))

    fig = go.Figure(go.Sankey(
        arrangement='snap',
        node=dict(
            label=labels,
            pad=10
        ),
        link=dict(
            arrowlen=15,
            source=df.SrcMacAddressId,
            target=df.DstMacAddressId,
            value=df.PacketSize
        )
    ))

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    header = "Src Destination Plot"
    description = """
    Visualization of packet flows between different MAC addresses.
    """
    return render_template('pages/chart.html', graphJSON=graphJSON, header=header, description=description)


@bp.route('/network-graph')
def network_graph():
    G = nx.random_geometric_graph(200, 0.125)
    edge_x = []
    edge_y = []

    for edge in G.edges():
        x0, y0 = G.nodes[edge[0]]['pos']
        x1, y1 = G.nodes[edge[1]]['pos']
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines'
    )

    node_x = []
    node_y = []

    for node in G.nodes():
        x, y = G.nodes[node]['pos']
        node_x.append(x)
        node_y.append(y)

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode='markers',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            reversescale=True,
            color=[],
            size=10,
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            ),
            line_width=2
        )
    )

    node_adjacencies = []
    node_text = []

    for node, adjacencies in enumerate(G.adjacency()):
        node_adjacencies.append(len(adjacencies[1]))
        node_text.append('# of connections: ' + str(len(adjacencies[1])))

    node_trace.marker.color = node_adjacencies
    node_trace.text = node_text

    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title='<br>Network Graph made with Python',
            titlefont_size=16,
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=40),
            annotations=[
                dict(
                    text="Python code: <a href='https://plotly.com/ipython-notebooks/network-graphs/'>https://plotly.com/ipython-notebooks/network-graphs/</a>",
                    showarrow=False,
                    xref="paper",
                    yref="paper",
                    x=0.005,
                    y=-0.002
                )
            ],
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
    )

    return render_template('pages/network_graph.html', graphJSON=fig.to_json())

app.register_blueprint(bp)

if __name__ == '__main__':
    app.run()
