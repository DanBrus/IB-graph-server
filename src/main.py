import uvicorn


def main():
    uvicorn.run(
        "graph_api:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
    )


if __name__ == "__main__":
    main()
